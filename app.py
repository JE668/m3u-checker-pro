import os, subprocess, json, threading, time, socket, datetime, uuid, csv, re, gzip, copy
import requests, urllib3, psutil
from flask import Flask, render_template, request, jsonify, send_from_directory, make_response, redirect
from urllib.parse import urlparse
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
from io import BytesIO
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Text, Boolean, JSON, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import StaticPool

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)

# --- 数据库配置 ---
DATA_DIR = "/app/data"
LOG_DIR = os.path.join(DATA_DIR, "log")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")  # 旧文件，仅迁移使用
ALIAS_FILE = os.path.join(DATA_DIR, "alias.txt")
DEMO_FILE = os.path.join(DATA_DIR, "demo.txt")
PENDING_FILE = os.path.join(DATA_DIR, "pending.json")  # 旧文件，仅迁移使用
EPG_CACHE_DIR = os.path.join(DATA_DIR, "epg_cache")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(EPG_CACHE_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "m3u_checker.db")
engine = create_engine(f'sqlite:///{DB_PATH}?check_same_thread=False', poolclass=StaticPool)
db_session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

# --- 定义模型 ---
class Subscription(Base):
    __tablename__ = 'subscriptions'
    id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    url = Column(Text, nullable=False)
    threads = Column(Integer, default=10)
    enabled = Column(Boolean, default=True)
    schedule_mode = Column(String(20), default='none')
    fixed_times = Column(String(500), default='')
    interval_hours = Column(Integer, default=1)
    res_filter = Column(JSON, default=['sd','720p','1080p','4k','8k'])
    created_at = Column(DateTime, default=datetime.datetime.now)

class Aggregate(Base):
    __tablename__ = 'aggregates'
    id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    subscription_ids = Column(JSON)
    strategy = Column(String(20), default='best_score')
    enabled = Column(Boolean, default=True)
    epg_aggregate_id = Column(String(50), nullable=True)
    last_update = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

class EPGAggregate(Base):
    __tablename__ = 'epg_aggregates'
    id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    sources = Column(JSON)
    cache_days = Column(Integer, default=3)
    update_interval = Column(Integer, default=24)
    enabled = Column(Boolean, default=True)
    last_update = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

class Setting(Base):
    __tablename__ = 'settings'
    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=False)

class ProbeResult(Base):
    __tablename__ = 'probe_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sub_id = Column(String(50), nullable=False, index=True)
    channel_name = Column(String(500), nullable=False)
    url = Column(Text, nullable=False)
    score = Column(Float, default=0)
    res_tag = Column(String(20))
    probe_time = Column(DateTime, default=datetime.datetime.now, index=True)

class PendingChannel(Base):
    __tablename__ = 'pending_channels'
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_name = Column(String(500), unique=True, nullable=False)
    count = Column(Integer, default=1)
    first_seen = Column(DateTime, default=datetime.datetime.now)
    sub_ids = Column(JSON)

# 创建表
Base.metadata.create_all(bind=engine)

# ---------- 旧数据迁移辅助函数（已删除JSON文件，保留但不再使用）----------
def migrate_from_json():
    pass  # 已手动删除JSON文件，无需迁移

# ---------- 别名加载与匹配 ----------
ALIAS_CACHE = None
ALIAS_MTIME = None

def load_aliases():
    global ALIAS_CACHE, ALIAS_MTIME
    if not os.path.exists(ALIAS_FILE):
        return {}
    mtime = os.path.getmtime(ALIAS_FILE)
    if ALIAS_CACHE is not None and ALIAS_MTIME == mtime:
        return ALIAS_CACHE
    aliases = {}
    with open(ALIAS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(',')
            main_name = parts[0].strip()
            alias_list = [a.strip() for a in parts[1:]]
            compiled = []
            for a in alias_list:
                if a.startswith('re:'):
                    try:
                        compiled.append(('re', re.compile(a[3:], re.IGNORECASE)))
                    except:
                        continue
                else:
                    compiled.append(('plain', a.lower()))
            aliases[main_name] = compiled
    ALIAS_CACHE = aliases
    ALIAS_MTIME = mtime
    return aliases

def match_channel_name(raw_name):
    aliases = load_aliases()
    raw_lower = raw_name.lower()
    for main_name, patterns in aliases.items():
        for ptype, p in patterns:
            if ptype == 'plain':
                if p in raw_lower:
                    return main_name, True
            else:
                if p.search(raw_name):
                    return main_name, True
    return raw_name, False

# ---------- 工具函数 ----------
def get_now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_today():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def format_duration(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def load_config():
    """从数据库加载配置，始终返回包含默认 settings 的字典"""
    config = {
        "subscriptions": [],
        "aggregates": [],
        "epg_aggregates": [],
        "settings": {
            "use_hwaccel": True,
            "epg_url": "http://epg.51zmt.top:12489/e.xml",
            "logo_base": "https://live.fanmingming.com/tv/"
        }
    }
    with db_session() as session:
        for sub in session.query(Subscription).all():
            config["subscriptions"].append({
                "id": sub.id,
                "name": sub.name,
                "url": sub.url,
                "threads": sub.threads,
                "enabled": sub.enabled,
                "schedule_mode": sub.schedule_mode,
                "fixed_times": sub.fixed_times,
                "interval_hours": sub.interval_hours,
                "res_filter": sub.res_filter
            })
        for agg in session.query(Aggregate).all():
            config["aggregates"].append({
                "id": agg.id,
                "name": agg.name,
                "subscription_ids": agg.subscription_ids,
                "strategy": agg.strategy,
                "enabled": agg.enabled,
                "epg_aggregate_id": agg.epg_aggregate_id
            })
        for epg in session.query(EPGAggregate).all():
            config["epg_aggregates"].append({
                "id": epg.id,
                "name": epg.name,
                "sources": epg.sources,
                "cache_days": epg.cache_days,
                "update_interval": epg.update_interval,
                "enabled": epg.enabled
            })
        for setting in session.query(Setting).all():
            config["settings"][setting.key] = setting.value
    return config

def save_config(config):
    """保存配置到数据库"""
    with db_session() as session:
        # 更新 subscriptions
        for sub_data in config["subscriptions"]:
            sub = session.get(Subscription, sub_data['id'])
            if sub:
                sub.name = sub_data['name']
                sub.url = sub_data['url']
                sub.threads = sub_data.get('threads', 10)
                sub.enabled = sub_data.get('enabled', True)
                sub.schedule_mode = sub_data.get('schedule_mode', 'none')
                sub.fixed_times = sub_data.get('fixed_times', '')
                sub.interval_hours = sub_data.get('interval_hours', 1)
                sub.res_filter = sub_data.get('res_filter', ['sd','720p','1080p','4k','8k'])
            else:
                session.add(Subscription(**sub_data))
        
        # 更新 aggregates
        for agg_data in config["aggregates"]:
            agg = session.get(Aggregate, agg_data['id'])
            if agg:
                agg.name = agg_data['name']
                agg.subscription_ids = agg_data.get('subscription_ids', [])
                agg.strategy = agg_data.get('strategy', 'best_score')
                agg.enabled = agg_data.get('enabled', True)
                agg.epg_aggregate_id = agg_data.get('epg_aggregate_id')
            else:
                session.add(Aggregate(**agg_data))
        
        # 更新 epg_aggregates
        for epg_data in config["epg_aggregates"]:
            epg = session.get(EPGAggregate, epg_data['id'])
            if epg:
                epg.name = epg_data['name']
                epg.sources = epg_data.get('sources', [])
                epg.cache_days = epg_data.get('cache_days', 3)
                epg.update_interval = epg_data.get('update_interval', 24)
                epg.enabled = epg_data.get('enabled', True)
            else:
                session.add(EPGAggregate(**epg_data))
        
        # 更新 settings
        for key, value in config["settings"].items():
            setting = session.get(Setting, key)
            if setting:
                setting.value = str(value)
            else:
                session.add(Setting(key=key, value=str(value)))
        session.commit()
    reschedule_all()
    reschedule_epg_all()

# ---------- CSV 日志记录 ----------
def write_log_csv(row_dict):
    csv_path = os.path.join(LOG_DIR, f"{get_today()}.csv")
    file_exists = os.path.isfile(csv_path)
    with file_lock:
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row_dict.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_dict)

# ---------- 全局状态 ----------
subs_status, ip_cache = {}, {}
aggregates_status = {}
epg_aggregates_status = {}
api_lock, log_lock, file_lock = threading.Lock(), threading.Lock(), threading.Lock()
scheduler = BackgroundScheduler()
scheduler.start()

# ---------- 地理定位（批量版）----------
def fetch_ip_locations_sync(sub_id, host_list):
    status = subs_status[sub_id]
    total = len(host_list)
    status["logs"].append(f"🌐 阶段 1/2: 正在检索 {total} 个节点的地理位置...")

    ips_to_query = []
    ip_to_host = {}
    for host in host_list:
        if host in ip_cache:
            continue
        try:
            ip = socket.gethostbyname(host)
            if ip in ip_cache:
                ip_cache[host] = ip_cache[ip]
                continue
            ips_to_query.append(ip)
            ip_to_host[ip] = host
        except:
            pass

    ips_to_query = list(set(ips_to_query))
    if not ips_to_query:
        status["logs"].append("✅ 阶段 1/2: 所有节点均已缓存，无需查询。")
        return

    batch_size = 100
    total_ips = len(ips_to_query)
    queried = 0
    for i in range(0, total_ips, batch_size):
        if status.get("stop_requested"):
            break
        batch = ips_to_query[i:i+batch_size]
        try:
            with api_lock:
                time.sleep(1.35)
                r = requests.post(
                    "http://ip-api.com/batch",
                    json=batch,
                    timeout=10,
                    verify=False
                ).json()
            for idx, info in enumerate(r):
                ip = batch[idx]
                if info.get('status') == 'success':
                    city = info.get('city', '未知')
                    isp = info.get('isp', '未知')
                    ip_cache[ip] = {"city": city, "isp": isp}
                    host = ip_to_host.get(ip)
                    if host:
                        ip_cache[host] = ip_cache[ip]
                        status["logs"].append(f"📍 定位分析 [{queried+idx+1}/{total_ips}]: {host} -> {city}")
                else:
                    ip_cache[ip] = {"city": "未知", "isp": "未知"}
            queried += len(batch)
        except Exception as e:
            status["logs"].append(f"⚠️ 批量查询失败: {str(e)}")
            for ip in batch:
                ip_cache[ip] = {"city": "未知", "isp": "未知"}
            queried += len(batch)

    status["logs"].append(f"✅ 阶段 1/2: 定位预检已完成。")

# ---------- FFprobe 探测（带调试）----------
def probe_stream(url, use_hw):
    accel_type = os.getenv("HW_ACCEL_TYPE", "vaapi").lower()
    device = os.getenv("VAAPI_DEVICE") or os.getenv("QSV_DEVICE") or "/dev/dri/renderD128"
    
    def run_f(hw, icon, mode_name):
        cmd = ['ffprobe', '-v', 'error', '-show_format', '-show_streams', '-print_format', 'json',
               '-user_agent', 'Mozilla/5.0', '-probesize', '5000000', '-analyzeduration', '5000000'] + hw + ['-i', url]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
            if r.returncode == 0:
                data = json.loads(r.stdout)
                streams = data.get('streams', [])
                v = next((s for s in streams if s['codec_type'] == 'video'), {})
                a = next((s for s in streams if s['codec_type'] == 'audio'), {})
                fmt = data.get('format', {})
                rb = fmt.get('bit_rate') or v.get('bit_rate') or "0"
                fps = "?"
                afps = v.get('avg_frame_rate', '0/0')
                if '/' in afps:
                    num, den = afps.split('/')
                    if int(den) > 0:
                        fps = str(round(int(num)/int(den)))
                if os.getenv('DEBUG_HW') == '1':
                    print(f"[HW] {mode_name} succeeded for {url}")
                return {
                    "res": f"{v.get('width','?')}x{v.get('height','?')}",
                    "h": v.get('height', 0),
                    "v_codec": v.get('codec_name', 'UNK').upper(),
                    "a_codec": a.get('codec_name', 'UNK').upper() if a else "无音频",
                    "fps": fps,
                    "br": f"{round(int(rb)/1024/1024, 2)}Mbps",
                    "icon": icon
                }
        except Exception as e:
            if os.getenv('DEBUG_HW') == '1':
                print(f"[HW] {mode_name} failed: {e}")
        return None
    
    if use_hw:
        hw_p = ['-hwaccel', 'vaapi', '-hwaccel_device', device, '-hwaccel_output_format', 'vaapi'] if accel_type == "vaapi" else ['-hwaccel', 'qsv', '-qsv_device', device]
        res = run_f(hw_p, "💎", "vaapi/qsv")
        if res:
            return res
        if os.getenv('DEBUG_HW') == '1':
            print(f"Hardware acceleration failed for {url}, falling back to software")
    return run_f([], "💻", "software")

# ---------- 待处理频道管理 ----------
def add_pending_channel(raw_name, sub_id):
    with db_session() as session:
        pc = session.query(PendingChannel).filter_by(raw_name=raw_name).first()
        if pc:
            pc.count += 1
            if sub_id not in pc.sub_ids:
                sub_ids = pc.sub_ids or []
                sub_ids.append(sub_id)
                pc.sub_ids = sub_ids
        else:
            pc = PendingChannel(raw_name=raw_name, count=1, sub_ids=[sub_id])
            session.add(pc)
        session.commit()

def append_alias(main_name, aliases):
    with open(ALIAS_FILE, 'a', encoding='utf-8') as f:
        line = f"{main_name}," + ",".join(aliases) + "\n"
        f.write(line)
    global ALIAS_CACHE, ALIAS_MTIME
    ALIAS_CACHE = None
    ALIAS_MTIME = None

def append_to_demo(channel_name, group_name):
    with open(DEMO_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{channel_name}\n")

# ---------- 单频道测试 ----------
def test_single_channel(sub_id, name, url, use_hw):
    status = subs_status[sub_id]

    if status.get("stop_requested"):
        with log_lock:
            status["current"] += 1
        return None

    parsed = urlparse(url)
    host = parsed.hostname
    hp = f"{host}:{parsed.port or (443 if parsed.scheme=='https' else 80)}"

    if hp in status["blacklisted_hosts"]:
        with log_lock:
            status["analytics"]["stability"]["banned"] += 1
            status["current"] += 1
        return None

    with log_lock:
        if hp not in status["summary_host"]:
            status["summary_host"][hp] = {"t": 0, "s": 0, "f": 0, "lat_sum": 0, "speed_sum": 0, "score_sum": 0}
        if hp not in status["consecutive_failures"]:
            status["consecutive_failures"][hp] = 0

    geo = None
    try:
        start_time = time.time()
        with requests.get(url, stream=True, timeout=8, verify=False,
                          headers={'User-Agent': 'Mozilla/5.0'}) as resp:
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}")
            latency = int((time.time() - start_time) * 1000)
            td, ss = 0, time.time()
            for chunk in resp.iter_content(chunk_size=128*1024):
                if status.get("stop_requested"):
                    return None
                td += len(chunk)
                if time.time() - ss > 2:
                    break
            speed = round((td * 8) / ((time.time() - ss) * 1024 * 1024), 2)

        meta = probe_stream(url, use_hw)
        if not meta:
            raise Exception("ProbeFail")

        geo = ip_cache.get(host) or {"city": "未知", "isp": "未知"}

        with log_lock:
            status["consecutive_failures"][hp] = 0
            status["success"] += 1
            status["summary_host"][hp]["s"] += 1
            if geo['city'] not in status["summary_city"]:
                status["summary_city"][geo['city']] = {"t": 0, "s": 0}
            status["summary_city"][geo['city']]["s"] += 1
            status["summary_host"][hp]["lat_sum"] += latency
            status["summary_host"][hp]["speed_sum"] += speed
            h = int(meta['h'])
            res_tag = "8K" if h >= 4320 else "4K" if h >= 2160 else "1080P" if h >= 1080 else "720P" if h >= 720 else "SD"
            status["analytics"]["res"][res_tag] += 1
            latency_cat = "<100ms" if latency < 100 else "<500ms" if latency < 500 else ">500ms"
            status["analytics"]["lat"][latency_cat] += 1
            status["analytics"]["v_codec"][meta['v_codec']] = status["analytics"]["v_codec"].get(meta['v_codec'], 0) + 1
            status["analytics"]["a_codec"][meta['a_codec']] = status["analytics"]["a_codec"].get(meta['a_codec'], 0) + 1
            status["analytics"]["stability"]["success"] += 1
            isp_name = geo.get('isp', '未知')
            status["analytics"]["isp"][isp_name] = status["analytics"]["isp"].get(isp_name, 0) + 1
            protocol = parsed.scheme
            if protocol in ('http', 'https'):
                status["analytics"]["protocol"][protocol] += 1
            br_value = float(meta['br'].replace('Mbps','').strip()) if 'Mbps' in meta['br'] else 0
            if br_value < 1:
                status["analytics"]["bitrate"]["<1M"] += 1
            elif br_value < 5:
                status["analytics"]["bitrate"]["1-5M"] += 1
            elif br_value < 10:
                status["analytics"]["bitrate"]["5-10M"] += 1
            else:
                status["analytics"]["bitrate"][">10M"] += 1

            score = h + speed * 5 - latency / 10
            status["summary_host"][hp]["score_sum"] += score
            fps_display = f"{meta['fps']} fps" if meta['fps'] != "?" else "?"
            msg = (f"✅ {name}: {meta['icon']}{meta['res']} | 🎬{meta['v_codec']} | 🎵{meta['a_codec']} | "
                   f"🎞️{fps_display} | 📊{speed}Mbps | ⏱️{latency}ms | 📍{geo['city']} | 🌐{hp}")
            status["logs"].append(msg)
            write_log_csv({
                "时间": get_now(),
                "任务": status['sub_name'],
                "状态": "成功",
                "频道": name,
                "分辨率": meta['res'],
                "视频编码": meta['v_codec'],
                "音频编码": meta['a_codec'],
                "FPS": meta['fps'],
                "延迟(ms)": latency,
                "网速(Mbps)": speed,
                "地区": geo['city'],
                "运营商": geo['isp'],
                "URL": url
            })
        
        # 检查是否未匹配别名，若是则加入待处理
        std_name, matched = match_channel_name(name)
        if not matched:
            add_pending_channel(name, sub_id)
        
        # 保存结果到数据库
        with db_session() as session:
            session.add(ProbeResult(
                sub_id=sub_id,
                channel_name=std_name,
                url=url,
                score=score,
                res_tag=res_tag.lower(),
                probe_time=datetime.datetime.now()
            ))
            session.commit()

        return {"name": name, "url": url, "score": score, "res_tag": res_tag.lower()}
    except Exception as e:
        with log_lock:
            status["consecutive_failures"][hp] += 1
            status["summary_host"][hp]["f"] += 1
            status["analytics"]["stability"]["fail"] += 1
            if status["consecutive_failures"][hp] >= 10:
                if hp not in status["blacklisted_hosts"]:
                    status["blacklisted_hosts"].add(hp)
                    status["logs"].append(f"⚠️ 熔断激活: 接口 {hp} 连续失败10次，已跳过。")
            if not status.get("stop_requested"):
                status["logs"].append(f"❌ {name}: 失败({str(e)}) | 🌐{hp}")
        return None
    finally:
        with log_lock:
            status["current"] += 1
            status["summary_host"][hp]["t"] += 1
            city = geo['city'] if geo else "未知城市"
            if city not in status["summary_city"]:
                status["summary_city"][city] = {"t": 0, "s": 0}
            status["summary_city"][city]["t"] += 1

# ---------- 任务运行 ----------
def run_task(sub_id):
    # 从数据库加载订阅信息
    with db_session() as session:
        sub = session.get(Subscription, sub_id)
        if not sub or not sub.enabled:
            return
        sub_name = sub.name
        sub_url = sub.url
        threads = sub.threads or 10
        res_filter = sub.res_filter or ["sd", "720p", "1080p", "4k", "8k"]
    
    # 从配置中获取 use_hw（安全方式）
    config = load_config()
    use_hw = config.get("settings", {}).get("use_hwaccel", True)

    if subs_status.get(sub_id, {}).get("running"):
        return
    start_ts = time.time()
    subs_status[sub_id] = {
        "running": True,
        "stop_requested": False,
        "total": 0,
        "current": 0,
        "success": 0,
        "sub_name": sub_name,
        "logs": [],
        "summary_host": {},
        "summary_city": {},
        "consecutive_failures": {},
        "blacklisted_hosts": set(),
        "analytics": {
            "res": {"SD": 0, "720P": 0, "1080P": 0, "4K": 0, "8K": 0},
            "lat": {"<100ms": 0, "<500ms": 0, ">500ms": 0},
            "v_codec": {},
            "a_codec": {},
            "stability": {"success": 0, "fail": 0, "banned": 0},
            "isp": {},
            "protocol": {"http": 0, "https": 0},
            "bitrate": {"<1M": 0, "1-5M": 0, "5-10M": 0, ">10M": 0}
        }
    }

    # 拉取订阅内容
    raw_channels = []
    try:
        r = requests.get(sub_url, timeout=15, verify=False)
        r.encoding = r.apparent_encoding
        content = r.text
        if "#EXTINF" in content:
            last_name = "未知频道"
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    continue
                if "#EXTINF" in line:
                    last_name = line.split(',')[-1].strip()
                elif "://" in line:
                    raw_channels.append((last_name, line))
        else:
            for line in content.split('\n'):
                if "," in line and "://" in line:
                    p = line.split(',')
                    raw_channels.append((p[0].strip(), p[1].strip()))
    except Exception as e:
        subs_status[sub_id]["logs"].append(f"❌ 订阅拉取失败: {e}")
        subs_status[sub_id]["running"] = False
        return

    raw_channels = list(set(raw_channels))
    total_num = len(raw_channels)
    subs_status[sub_id]["total"] = total_num

    if total_num > 0:
        unique_hosts = list(set([urlparse(c[1]).hostname for c in raw_channels if c[1]]))
        fetch_ip_locations_sync(sub_id, unique_hosts)

        subs_status[sub_id]["logs"].append(f"🚀 阶段 2/2: 开始探测 {total_num} 个频道...")

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(test_single_channel, sub_id, n, u, use_hw) for n, u in raw_channels]
            valid_raw = []
            for f in futures:
                if subs_status[sub_id].get("stop_requested"):
                    pass
                try:
                    res = f.result(timeout=30)
                    if res:
                        valid_raw.append(res)
                except Exception as e:
                    subs_status[sub_id]["logs"].append(f"⚠️ 任务异常: {str(e)}")
    else:
        valid_raw = []

    valid_list = [c for c in valid_raw if c['res_tag'] in res_filter]
    valid_list.sort(key=lambda x: x['score'], reverse=True)

    status = subs_status[sub_id]
    duration = format_duration(time.time() - start_ts)
    update_ts = get_now()

    # 生成报告
    status["logs"].append(" ")
    status["logs"].append("📜 ==================== 探测结算报告 ====================")
    status["logs"].append(f"⏱️ 任务总耗时: {duration} | 有效源: {len(valid_list)} / 成功探测: {status['success']}")
    status["logs"].append("🏙️ --- 地区连通汇总 ---")
    sc = sorted([i for i in status["summary_city"].items() if i[1]['t'] > 0],
                key=lambda x: x[1]['s']/x[1]['t'], reverse=True)
    for c, d in sc:
        status["logs"].append(f"📍 {c:<30} | 有效率: {round(d['s']/d['t']*100, 1)}% ({d['s']}/{d['t']})")
    status["logs"].append("📡 --- 接口质量全表 (按评分) ---")
    ah = {k: v for k, v in status["summary_host"].items() if k not in status["blacklisted_hosts"] and v['t'] > 0}
    sh = sorted(ah.items(), key=lambda x: x[1]['score_sum']/x[1]['s'] if x[1]['s'] > 0 else 0, reverse=True)
    for h, d in sh:
        al = int(d['lat_sum']/d['s']) if d['s'] > 0 else 0
        aspd = round(d['speed_sum']/d['s'], 2) if d['s'] > 0 else 0
        status["logs"].append(f"{'⭐️' if d['s']/d['t'] > 0.8 else '📡'} {h:<24} | ⏱️{al}ms | 🚀{aspd}Mbps | 有效率: {round(d['s']/d['t']*100, 1)}%")
    if status["blacklisted_hosts"]:
        status["logs"].append("🚫 --- 已熔断的接口清单 ---")
        for bh in status["blacklisted_hosts"]:
            status["logs"].append(f"❌ {bh} (连续10次失败)")
    status["logs"].append("📊 --- 运营商分布 ---")
    isp_sorted = sorted(status["analytics"]["isp"].items(), key=lambda x: x[1], reverse=True)[:10]
    for isp, count in isp_sorted:
        status["logs"].append(f"📡 {isp}: {count}")
    status["logs"].append("🌐 --- 协议比例 ---")
    for proto, count in status["analytics"]["protocol"].items():
        status["logs"].append(f"{proto.upper()}: {count}")
    status["logs"].append("📈 --- 比特率分段 ---")
    for br_range, count in status["analytics"]["bitrate"].items():
        status["logs"].append(f"{br_range}: {count}")
    status["logs"].append("======================================================")
    status["logs"].append(f"🏁 任务完成时间: {get_now()}")

    # 输出 M3U 和 TXT
    try:
        m3u_p = os.path.join(OUTPUT_DIR, f"{sub_id}.m3u")
        txt_p = os.path.join(OUTPUT_DIR, f"{sub_id}.txt")
        epg = config.get("settings", {}).get("epg_url", "")
        logo = config.get("settings", {}).get("logo_base", "")
        with open(m3u_p, 'w', encoding='utf-8') as fm:
            fm.write(f"#EXTM3U x-tvg-url=\"{epg}\"\n# Updated: {update_ts}\n# Duration: {duration}\n")
            for c in valid_list:
                fm.write(f"#EXTINF:-1 tvg-logo=\"{logo}{c['name']}.png\",{c['name']}\n{c['url']}\n")
        with open(txt_p, 'w', encoding='utf-8') as ft:
            ft.write(f"# Updated: {update_ts}\n# Duration: {duration}\n")
            for c in valid_list:
                ft.write(f"{c['name']},{c['url']}\n")
    except Exception as e:
        status["logs"].append(f"⚠️ 写入文件失败: {e}")

    status["running"] = False

    # 触发包含此订阅的聚合任务自动更新
    with db_session() as session:
        for agg in session.query(Aggregate).filter(Aggregate.subscription_ids.contains(sub_id)).all():
            threading.Thread(target=run_aggregate, args=(agg.id,), kwargs={"auto": True}).start()

# ---------- 聚合任务（保留所有URL，并将未在demo.txt中的频道归入“其他频道”）----------
def run_aggregate(agg_id, auto=False):
    if aggregates_status.get(agg_id, {}).get("running"):
        return
    aggregates_status[agg_id] = {"running": True, "logs": []}
    
    def log(msg):
        ts = get_now()
        aggregates_status[agg_id]["logs"].append(f"{ts} - {msg}")
    
    log(f"🚀 聚合任务开始 (自动: {auto})")
    
    with db_session() as session:
        agg = session.get(Aggregate, agg_id)
        if not agg or not agg.enabled:
            log("❌ 聚合配置不存在或未启用")
            aggregates_status[agg_id]["running"] = False
            return

        log(f"📋 聚合名称: {agg.name}")
        log(f"📦 包含订阅: {', '.join(agg.subscription_ids or [])}")

        # 获取所有探测结果
        results = []
        for sid in agg.subscription_ids or []:
            sub_results = session.query(ProbeResult).filter(ProbeResult.sub_id == sid).all()
            results.extend(sub_results)
        log(f"📊 从数据库读取 {len(results)} 条原始探测结果")

        # 按标准名称分组，每个标准名称对应一个列表
        channel_map = {}
        for r in results:
            std_name, matched = match_channel_name(r.channel_name)
            if std_name not in channel_map:
                channel_map[std_name] = []
            channel_map[std_name].append({
                "name": std_name,
                "url": r.url,
                "score": r.score,
                "res_tag": r.res_tag
            })

        # 对每个标准名称下的URL按评分降序排序
        for name in channel_map:
            channel_map[name].sort(key=lambda x: x['score'], reverse=True)

        log(f"📊 聚合后得到 {len(channel_map)} 个标准频道，共计 {sum(len(v) for v in channel_map.values())} 个URL")

    # 读取 demo.txt 获取顺序和分组信息
    ordered_names = []
    group_map = {}
    if os.path.exists(DEMO_FILE):
        current_group = "未分组"
        with open(DEMO_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ',#genre#' in line:
                    current_group = line.split(',')[0].strip()
                    log(f"📂 识别分组: {current_group}")
                else:
                    name = line
                    ordered_names.append(name)
                    group_map[name] = current_group
        log(f"📋 从 demo.txt 加载了 {len(ordered_names)} 个频道顺序")
    else:
        # 如果没有demo.txt，则使用所有标准名称按字母排序
        ordered_names = sorted(channel_map.keys())
        log(f"📋 未找到 demo.txt，使用字母顺序")

    # 按顺序生成最终列表（先处理 demo.txt 中的频道）
    final_list = []
    for name in ordered_names:
        if name in channel_map:
            for item in channel_map[name]:
                item["group"] = group_map.get(name, "未分组")
                final_list.append(item)

    # 处理不在 demo.txt 中的频道，归入“其他频道”分组
    remaining_names = set(channel_map.keys()) - set(ordered_names)
    if remaining_names:
        log(f"📦 发现 {len(remaining_names)} 个频道不在 demo.txt 中，将归入“其他频道”分组")
        for name in sorted(remaining_names):  # 按字母排序
            for item in channel_map[name]:
                item["group"] = "其他频道"
                final_list.append(item)

    log(f"✅ 最终生成 {len(final_list)} 个有效链接")

    # 确定使用的 EPG URL
    config = load_config()
    epg_url = config.get("settings", {}).get("epg_url", "")
    epg_agg_id = agg.epg_aggregate_id
    if epg_agg_id:
        with db_session() as session:
            epg_agg = session.get(EPGAggregate, epg_agg_id)
            if epg_agg:
                # 使用相对路径，避免 request 上下文问题
                epg_url = f"/epg/{epg_agg_id}.xml"
                log(f"📺 使用 EPG 聚合: {epg_agg.name} -> {epg_url}")
            else:
                log(f"⚠️ 指定的 EPG 聚合不存在，使用全局 EPG")
    else:
        log(f"📺 使用全局 EPG: {epg_url}")

    # 生成输出文件
    update_ts = get_now()
    logo_base = config.get("settings", {}).get("logo_base", "")
    m3u_path = os.path.join(OUTPUT_DIR, f"aggregate_{agg_id}.m3u")
    txt_path = os.path.join(OUTPUT_DIR, f"aggregate_{agg_id}.txt")
    
    with open(m3u_path, 'w', encoding='utf-8') as fm:
        fm.write(f"#EXTM3U x-tvg-url=\"{epg_url}\"\n# Updated: {update_ts}\n")
        for c in final_list:
            tvg_name = c['name']
            tvg_logo = f"{logo_base}{tvg_name}.png"
            group_title = c.get('group', '未分组')
            fm.write(f"#EXTINF:-1 tvg-name=\"{tvg_name}\" tvg-logo=\"{tvg_logo}\" group-title=\"{group_title}\",{tvg_name}\n")
            fm.write(f"{c['url']}\n")

    with open(txt_path, 'w', encoding='utf-8') as ft:
        ft.write(f"# Updated: {update_ts}\n")
        for c in final_list:
            ft.write(f"{c['name']},{c['url']}\n")

    log(f"💾 文件已写入: {m3u_path}, {txt_path}")

    # 更新聚合最后更新时间
    with db_session() as session:
        agg = session.get(Aggregate, agg_id)
        if agg:
            agg.last_update = datetime.datetime.now()
            session.commit()

    log(f"🏁 聚合任务完成")
    aggregates_status[agg_id]["running"] = False

# ---------- EPG 聚合（增强版）----------
def run_epg_aggregate(epg_agg_id, auto=False):
    try:
        if epg_aggregates_status.get(epg_agg_id, {}).get("running"):
            return
        epg_aggregates_status[epg_agg_id] = {"running": True, "logs": []}
        
        def log(msg):
            ts = get_now()
            epg_aggregates_status[epg_agg_id]["logs"].append(f"{ts} - {msg}")
        
        log(f"📺 EPG 聚合任务开始 (自动: {auto})")
        
        # 从数据库获取 EPG 聚合配置
        with db_session() as session:
            epg_agg = session.get(EPGAggregate, epg_agg_id)
            if not epg_agg or not epg_agg.enabled:
                log("❌ EPG 聚合配置不存在或未启用")
                epg_aggregates_status[epg_agg_id]["running"] = False
                return

            log(f"📋 EPG 聚合名称: {epg_agg.name}")
            log(f"🔗 源列表: {', '.join(epg_agg.sources)}")
            cache_days = epg_agg.cache_days or 3
            log(f"📅 缓存天数: {cache_days}")

        today = datetime.date.today()
        date_list = [today + datetime.timedelta(days=i) for i in range(-1, cache_days)]
        date_strs = [d.strftime('%Y%m%d') for d in date_list]
        log(f"📅 需要包含的日期: {', '.join(date_strs)}")

        programmes = {}
        channels_dict = {}

        # 下载并解析每个源
        for idx, source_url in enumerate(epg_agg.sources):
            log(f"⬇️ 正在下载源 {idx+1}: {source_url}")
            try:
                resp = requests.get(source_url, timeout=30)
                if resp.status_code != 200:
                    log(f"⚠️ 源 {source_url} 返回状态码 {resp.status_code}，跳过")
                    continue
                content = resp.content

                # 处理可能为 gzip 压缩的内容
                is_gz = source_url.endswith('.gz')
                if is_gz:
                    try:
                        buf = BytesIO(content)
                        with gzip.GzipFile(fileobj=buf) as gz_file:
                            content = gz_file.read()
                        log(f"📦 检测到 gzip 压缩，已解压")
                    except Exception as e:
                        log(f"⚠️ 解压失败: {str(e)}，将作为普通 XML 尝试解析")
                        content = resp.content  # 恢复原始内容

                try:
                    tree = ET.parse(BytesIO(content))
                    root = tree.getroot()
                except Exception as e:
                    log(f"❌ 解析 XML 失败: {str(e)}")
                    continue

                channels_added = 0
                for channel in root.findall('channel'):
                    ch_id = channel.get('id')
                    if ch_id:
                        std_name, matched = match_channel_name(ch_id)
                        if ch_id not in channels_dict:
                            channels_dict[ch_id] = (channel, std_name if matched else None)
                            channels_added += 1
                if channels_added > 0:
                    log(f"📺 源 {idx+1} 添加了 {channels_added} 个频道")

                count = 0
                for prog in root.findall('programme'):
                    start = prog.get('start')
                    channel = prog.get('channel')
                    title_elem = prog.find('title')
                    title = title_elem.text if title_elem is not None else ''
                    if start and len(start) >= 8:
                        prog_date = start[:8]
                        if prog_date in date_strs:
                            key = (channel, start, title)
                            if key not in programmes:
                                programmes[key] = prog
                                count += 1
                log(f"➕ 源 {idx+1} 添加了 {count} 个节目")
            except Exception as e:
                log(f"❌ 下载源 {source_url} 失败: {str(e)}")

        log(f"📊 共收集到 {len(channels_dict)} 个频道，{len(programmes)} 个节目")

        new_root = ET.Element('tv')
        for ch_id, (ch_elem, std_name) in channels_dict.items():
            new_ch = copy.deepcopy(ch_elem)
            if std_name:
                dn = ET.SubElement(new_ch, 'display-name')
                dn.text = std_name
            new_root.append(new_ch)
        for prog in programmes.values():
            new_root.append(prog)

        update_ts = get_now()
        xml_path = os.path.join(OUTPUT_DIR, f"epg_{epg_agg_id}.xml")
        tree = ET.ElementTree(new_root)
        tree.write(xml_path, encoding='utf-8', xml_declaration=True)
        log(f"💾 XML 已保存: {xml_path}")

        with db_session() as session:
            epg = session.get(EPGAggregate, epg_agg_id)
            if epg:
                epg.last_update = datetime.datetime.now()
                session.commit()

        epg_status = {
            "update_time": update_ts,
            "total": len(programmes),
            "channels": len(channels_dict),
            "sources": epg_agg.sources,
            "files": {"xml": f"/epg/{epg_agg_id}.xml"}
        }
        status_path = os.path.join(OUTPUT_DIR, f"epg_{epg_agg_id}_status.json")
        with open(status_path, 'w', encoding='utf-8') as f:
            json.dump(epg_status, f, ensure_ascii=False)

        log(f"🏁 EPG 聚合任务完成")
        epg_aggregates_status[epg_agg_id]["running"] = False
    except Exception as e:
        epg_aggregates_status[epg_agg_id]["running"] = False
        log(f"❌ 聚合任务异常: {str(e)}")

# ---------- 计划任务调度 ----------
def clear_sub_jobs(sub_id):
    for job in scheduler.get_jobs():
        if job.id.startswith(sub_id):
            scheduler.remove_job(job.id)

def schedule_subscription(sub):
    sub_id = sub.id
    clear_sub_jobs(sub_id)
    if not sub.enabled:
        return
    mode = sub.schedule_mode
    if mode == "none":
        return
    elif mode == "fixed":
        times = (sub.fixed_times or "").split(",")
        for t in times:
            t = t.strip()
            if not t:
                continue
            try:
                hour, minute = map(int, t.split(':'))
                job_id = f"{sub_id}_fixed_{hour:02d}{minute:02d}"
                scheduler.add_job(
                    func=run_task,
                    args=[sub_id],
                    trigger='cron',
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    replace_existing=True
                )
            except Exception as e:
                app.logger.error(f"调度 fixed 任务失败 {sub_id} {t}: {e}")
    elif mode == "interval":
        hours = sub.interval_hours or 1
        job_id = f"{sub_id}_interval"
        scheduler.add_job(
            func=run_task,
            args=[sub_id],
            trigger='interval',
            hours=hours,
            id=job_id,
            replace_existing=True
        )

def reschedule_all():
    with db_session() as session:
        for sub in session.query(Subscription).all():
            schedule_subscription(sub)

def clear_epg_jobs(epg_agg_id):
    for job in scheduler.get_jobs():
        if job.id.startswith(f"epg_{epg_agg_id}"):
            scheduler.remove_job(job.id)

def schedule_epg_aggregation(epg_agg):
    epg_id = epg_agg.id
    clear_epg_jobs(epg_id)
    if not epg_agg.enabled:
        return
    interval = epg_agg.update_interval or 24
    job_id = f"epg_{epg_id}_interval"
    scheduler.add_job(
        func=run_epg_aggregate,
        args=[epg_id],
        kwargs={"auto": True},
        trigger='interval',
        hours=interval,
        id=job_id,
        replace_existing=True
    )

def reschedule_epg_all():
    with db_session() as session:
        for epg_agg in session.query(EPGAggregate).all():
            schedule_epg_aggregation(epg_agg)

# ---------- Flask 路由 ----------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/m3u_aggregate')
def m3u_aggregate_page():
    return render_template('m3u_aggregate.html')

@app.route('/epg_aggregate')
def epg_aggregate_page():
    return render_template('epg_aggregate.html')

@app.route('/pending')
def pending_page():
    return render_template('pending.html')

@app.route('/api/sys_info')
def sys_info():
    try:
        gpu = 0
        if os.path.exists("/sys/class/drm/card0/device/gpu_busy_percent"):
            with open("/sys/class/drm/card0/device/gpu_busy_percent", 'r') as f:
                gpu = int(f.read().strip())
        return jsonify({
            "cpu": psutil.cpu_percent(),
            "ram": psutil.virtual_memory().percent,
            "gpu": gpu,
            "gpu_active": any(s.get("running") for s in subs_status.values())
        })
    except:
        return jsonify({"cpu": 0, "ram": 0, "gpu": 0})

@app.route('/api/network_test')
def network_test():
    res = {"v4": {"status": False, "ip": ""}, "v6": {"status": False, "ip": ""}}
    
    ipv4_services = [
        "https://api4.ipify.org?format=json",
        "https://api.ip.sb/ip?format=json",
        "https://ipv4.icanhazip.com/"
    ]
    for service in ipv4_services:
        try:
            if service.endswith('.com/'):
                r = requests.get(service, timeout=8)
                ip = r.text.strip()
                if ip:
                    res["v4"] = {"status": True, "ip": ip}
                    break
            else:
                r = requests.get(service, timeout=8).json()
                ip = r.get('ip') or r.get('IPv4')
                if ip:
                    res["v4"] = {"status": True, "ip": ip}
                    break
        except:
            continue

    try:
        r6 = requests.get("https://api6.ipify.org?format=json", timeout=8).json()
        res["v6"] = {"status": True, "ip": r6['ip']}
    except:
        pass

    return jsonify(res)

@app.route('/api/subs', methods=['GET', 'POST'])
def handle_subs():
    if request.method == 'POST':
        data = request.json
        with db_session() as session:
            if not data.get("id"):
                data["id"] = str(uuid.uuid4())[:8]
                sub = Subscription(**data)
                session.add(sub)
            else:
                sub = session.get(Subscription, data["id"])
                if sub:
                    for k, v in data.items():
                        setattr(sub, k, v)
            session.commit()
        return jsonify({"status": "ok"})
    else:
        with db_session() as session:
            subs = [{
                "id": s.id, "name": s.name, "url": s.url, "threads": s.threads,
                "enabled": s.enabled, "schedule_mode": s.schedule_mode,
                "fixed_times": s.fixed_times, "interval_hours": s.interval_hours,
                "res_filter": s.res_filter
            } for s in session.query(Subscription).all()]
            settings = {s.key: s.value for s in session.query(Setting).all()}
            return jsonify({"subs": subs, "settings": settings})

@app.route('/api/status/<sub_id>')
def get_status(sub_id):
    limit = request.args.get('limit', default=150, type=int)
    if sub_id in subs_status:
        s = subs_status[sub_id]
        return jsonify({
            "running": s["running"],
            "logs": s["logs"][-limit:],
            "total": s["total"],
            "current": s["current"],
            "success": s["success"],
            "banned_count": len(s.get("blacklisted_hosts", [])),
            "analytics": s["analytics"]
        })
    archive_path = os.path.join(OUTPUT_DIR, f"last_status_{sub_id}.json")
    if os.path.exists(archive_path):
        with open(archive_path, 'r', encoding='utf-8') as f:
            d = json.load(f)
            return jsonify({
                "running": False,
                "logs": d["logs"][-limit:],
                "total": d["stats"]["total"],
                "current": d["stats"]["current"],
                "success": d["stats"]["success"],
                "banned_count": d["stats"]["banned"],
                "analytics": d["analytics"]
            })
    return jsonify({"running": False, "logs": [], "total": 0, "current": 0, "success": 0, "banned_count": 0, "analytics": {}})

@app.route('/api/start/<sub_id>')
def start_api(sub_id):
    threading.Thread(target=run_task, args=(sub_id,)).start()
    return jsonify({"status": "ok"})

@app.route('/api/stop/<sub_id>')
def stop_api(sub_id):
    if sub_id in subs_status:
        subs_status[sub_id]["stop_requested"] = True
    return jsonify({"status": "ok"})

@app.route('/api/settings', methods=['POST'])
def save_settings():
    data = request.json
    with db_session() as session:
        for key, value in data.items():
            setting = session.get(Setting, key)
            if setting:
                setting.value = str(value)
            else:
                session.add(Setting(key=key, value=str(value)))
        session.commit()
    return jsonify({"status": "ok"})

@app.route('/api/hw_test')
def hw_test():
    try:
        r = subprocess.run(['vainfo'], capture_output=True, text=True, timeout=5)
        out = r.stdout + r.stderr
        ready = "va_openDriver() returns 0" in out
        codecs = []
        mapping = {"H264": "H264", "HEVC (H.265)": "HEVC|H265", "VP9": "VP9", "MPEG2": "MPEG2"}
        for k, v in mapping.items():
            if any(x in out.upper() for x in v.split('|')):
                codecs.append(k)
        return jsonify({
            "status": "success" if ready else "error",
            "message": "✅ GPU加速就绪" if ready else "❌ 驱动异常",
            "codecs": codecs,
            "raw": out
        })
    except Exception as e:
        return jsonify({"status": "error", "raw": str(e)})

@app.route('/api/subs/delete/<sub_id>')
def delete_sub(sub_id):
    with db_session() as session:
        sub = session.get(Subscription, sub_id)
        if sub:
            session.delete(sub)
            session.commit()
    clear_sub_jobs(sub_id)
    return jsonify({"status": "ok"})

@app.route('/sub/<sub_id>.<ext>')
def get_sub_file(sub_id, ext):
    return send_from_directory(OUTPUT_DIR, f"{sub_id}.{ext}")

# ---------- 聚合相关 API ----------
@app.route('/api/aggregates', methods=['GET', 'POST'])
def api_aggregates():
    if request.method == 'POST':
        data = request.json
        with db_session() as session:
            if not data.get("id"):
                data["id"] = str(uuid.uuid4())[:8]
                agg = Aggregate(**data)
                session.add(agg)
            else:
                agg = session.get(Aggregate, data["id"])
                if agg:
                    for k, v in data.items():
                        setattr(agg, k, v)
            session.commit()
        return jsonify({"status": "ok"})
    else:
        with db_session() as session:
            result = []
            for agg in session.query(Aggregate).all():
                agg_dict = {
                    "id": agg.id,
                    "name": agg.name,
                    "subscription_ids": agg.subscription_ids,
                    "strategy": agg.strategy,
                    "enabled": agg.enabled,
                    "epg_aggregate_id": agg.epg_aggregate_id,
                    "last_update": agg.last_update.strftime('%Y-%m-%d %H:%M:%S') if agg.last_update else "从未"
                }
                result.append(agg_dict)
            return jsonify(result)

@app.route('/api/aggregate/run/<agg_id>')
def run_aggregate_api(agg_id):
    threading.Thread(target=run_aggregate, args=(agg_id,), kwargs={"auto": False}).start()
    return jsonify({"status": "ok"})

@app.route('/api/aggregate/log/<agg_id>')
def get_aggregate_log(agg_id):
    logs = aggregates_status.get(agg_id, {}).get("logs", [])
    return jsonify({"logs": logs})

@app.route('/api/aggregate/delete/<agg_id>')
def delete_aggregate(agg_id):
    with db_session() as session:
        agg = session.get(Aggregate, agg_id)
        if agg:
            session.delete(agg)
            session.commit()
    return jsonify({"status": "ok"})

@app.route('/aggregate/<agg_id>.<ext>')
def get_aggregate_file(agg_id, ext):
    return send_from_directory(OUTPUT_DIR, f"aggregate_{agg_id}.{ext}")

# ---------- EPG 聚合相关 API ----------
@app.route('/api/epg_aggregates', methods=['GET', 'POST'])
def api_epg_aggregates():
    if request.method == 'POST':
        data = request.json
        with db_session() as session:
            if not data.get("id"):
                data["id"] = str(uuid.uuid4())[:8]
                epg = EPGAggregate(**data)
                session.add(epg)
            else:
                epg = session.get(EPGAggregate, data["id"])
                if epg:
                    for k, v in data.items():
                        setattr(epg, k, v)
            session.commit()
        return jsonify({"status": "ok"})
    else:
        with db_session() as session:
            result = []
            for epg in session.query(EPGAggregate).all():
                epg_dict = {
                    "id": epg.id,
                    "name": epg.name,
                    "sources": epg.sources,
                    "cache_days": epg.cache_days,
                    "update_interval": epg.update_interval,
                    "enabled": epg.enabled,
                    "last_update": epg.last_update.strftime('%Y-%m-%d %H:%M:%S') if epg.last_update else "从未"
                }
                result.append(epg_dict)
            return jsonify(result)

@app.route('/api/epg_aggregate/run/<epg_id>')
def run_epg_aggregate_api(epg_id):
    threading.Thread(target=run_epg_aggregate, args=(epg_id,), kwargs={"auto": False}).start()
    return jsonify({"status": "ok"})

@app.route('/api/epg_aggregate/log/<epg_id>')
def get_epg_aggregate_log(epg_id):
    logs = epg_aggregates_status.get(epg_id, {}).get("logs", [])
    return jsonify({"logs": logs})

@app.route('/api/epg_aggregate/delete/<epg_id>')
def delete_epg_aggregate(epg_id):
    with db_session() as session:
        epg = session.get(EPGAggregate, epg_id)
        if epg:
            session.delete(epg)
            session.commit()
    return jsonify({"status": "ok"})

@app.route('/epg/<epg_id>.xml')
def get_epg_xml(epg_id):
    filename = f"epg_{epg_id}.xml"
    return send_from_directory(OUTPUT_DIR, filename)

# ---------- EPG 频道检查 API ----------
@app.route('/api/epg_check/<epg_id>')
def epg_check(epg_id):
    channel = request.args.get('channel', '').strip()
    if not channel:
        return jsonify({"error": "频道名称不能为空"}), 400
    xml_path = os.path.join(OUTPUT_DIR, f"epg_{epg_id}.xml")
    if not os.path.exists(xml_path):
        return jsonify({"exists": False, "message": "EPG 文件不存在"})
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        channels = []
        for ch in root.findall('channel'):
            ch_id = ch.get('id', '')
            if channel.lower() in ch_id.lower():
                channels.append(ch_id)
            for dn in ch.findall('display-name'):
                if channel.lower() in (dn.text or '').lower():
                    channels.append(ch_id)
        programmes = []
        for prog in root.findall('programme'):
            prog_ch = prog.get('channel', '')
            if channel.lower() in prog_ch.lower():
                programmes.append({
                    "channel": prog_ch,
                    "start": prog.get('start'),
                    "title": prog.findtext('title', '')
                })
        return jsonify({
            "channel_exists": len(channels) > 0,
            "programme_count": len(programmes),
            "matched_channels": list(set(channels)),
            "matched_programmes_sample": programmes[:5]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- 待处理频道 API ----------
@app.route('/api/groups')
def get_groups():
    groups = []
    if os.path.exists(DEMO_FILE):
        with open(DEMO_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and ',#genre#' in line:
                    group_name = line.split(',')[0].strip()
                    groups.append(group_name)
    return jsonify(groups)

@app.route('/api/pending', methods=['GET'])
def get_pending():
    with db_session() as session:
        pendings = session.query(PendingChannel).order_by(PendingChannel.count.desc()).all()
        return jsonify([{
            "name": p.raw_name,
            "count": p.count,
            "first_seen": p.first_seen.strftime('%Y-%m-%d %H:%M:%S'),
            "sub_ids": p.sub_ids
        } for p in pendings])

@app.route('/api/channel_names')
def get_channel_names():
    """返回所有已知的标准名（用于别名输入建议）"""
    names = set()
    # 从 alias.txt 获取主名
    if os.path.exists(ALIAS_FILE):
        with open(ALIAS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                if parts:
                    names.add(parts[0].strip())
    # 从 demo.txt 获取频道名（非分组行）
    if os.path.exists(DEMO_FILE):
        with open(DEMO_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ',#genre#' not in line:
                    names.add(line)
    return jsonify(sorted(names))

@app.route('/api/alias/<main_name>')
def get_aliases(main_name):
    """返回指定标准名在 alias.txt 中的所有别名（不包括主名本身）"""
    aliases = []
    if os.path.exists(ALIAS_FILE):
        with open(ALIAS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                if parts and parts[0].strip() == main_name:
                    aliases = [a.strip() for a in parts[1:] if a.strip()]
                    break
    return jsonify(aliases)

@app.route('/api/pending/ignore', methods=['POST'])
def ignore_pending():
    data = request.json
    name = data.get('name')
    if not name:
        return jsonify({"error": "缺少频道名"}), 400
    with db_session() as session:
        pc = session.query(PendingChannel).filter_by(raw_name=name).first()
        if pc:
            session.delete(pc)
            session.commit()
    return jsonify({"status": "ok"})

@app.route('/api/pending/set_alias', methods=['POST'])
def set_alias():
    data = request.json
    raw_name = data.get('raw_name')
    main_name = data.get('main_name')
    aliases = data.get('aliases', [])
    if not raw_name or not main_name:
        return jsonify({"error": "缺少必要参数"}), 400
    all_aliases = list(set([raw_name] + aliases))
    append_alias(main_name, all_aliases)
    with db_session() as session:
        pc = session.query(PendingChannel).filter_by(raw_name=raw_name).first()
        if pc:
            session.delete(pc)
            session.commit()
    return jsonify({"status": "ok"})

@app.route('/api/pending/set_group', methods=['POST'])
def set_group():
    data = request.json
    channel_name = data.get('channel_name')
    group_name = data.get('group_name')
    if not channel_name or not group_name:
        return jsonify({"error": "缺少必要参数"}), 400
    append_to_demo(channel_name, group_name)
    with db_session() as session:
        pc = session.query(PendingChannel).filter_by(raw_name=channel_name).first()
        if pc:
            session.delete(pc)
            session.commit()
    return jsonify({"status": "ok"})

# ---------- 启动时初始化调度 ----------
with app.app_context():
    reschedule_all()
    reschedule_epg_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5123)
