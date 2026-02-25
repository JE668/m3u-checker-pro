import os, subprocess, json, threading, time, socket, datetime, uuid, csv, re, gzip
import requests, urllib3, psutil
from flask import Flask, render_template, request, jsonify, send_from_directory, make_response, redirect
from urllib.parse import urlparse
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
from io import BytesIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)

# --- 路径配置 ---
DATA_DIR = "/app/data"
LOG_DIR = os.path.join(DATA_DIR, "log")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
ALIAS_FILE = os.path.join(DATA_DIR, "alias.txt")
DEMO_FILE = os.path.join(DATA_DIR, "demo.txt")
EPG_CACHE_DIR = os.path.join(DATA_DIR, "epg_cache")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(EPG_CACHE_DIR, exist_ok=True)

subs_status, ip_cache = {}, {}
aggregates_status = {}      # 聚合任务运行状态
epg_aggregates_status = {}  # EPG聚合任务运行状态
api_lock, log_lock, file_lock = threading.Lock(), threading.Lock(), threading.Lock()
scheduler = BackgroundScheduler()
scheduler.start()

# ---------- 别名加载与匹配（增强版，支持正则）----------
ALIAS_CACHE = None
ALIAS_MTIME = None

def load_aliases():
    """加载 alias.txt，返回 {标准名称: [编译好的模式列表]}"""
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
    """根据别名库匹配标准名称，返回 (标准名, 是否匹配)"""
    aliases = load_aliases()
    raw_lower = raw_name.lower()
    for main_name, patterns in aliases.items():
        for ptype, p in patterns:
            if ptype == 'plain':
                if p in raw_lower:
                    return main_name, True
            else:  # regex
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
    default = {
        "subscriptions": [],
        "aggregates": [],
        "epg_aggregates": [],
        "settings": {
            "use_hwaccel": True,
            "epg_url": "http://epg.51zmt.top:12489/e.xml",
            "logo_base": "https://live.fanmingming.com/tv/"
        }
    }
    if not os.path.exists(CONFIG_FILE):
        return default
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
            if "settings" not in d:
                d["settings"] = default["settings"]
            if "aggregates" not in d:
                d["aggregates"] = []
            if "epg_aggregates" not in d:
                d["epg_aggregates"] = []
            return d
    except:
        return default

def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
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
            # 新增统计
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
    config = load_config()
    sub = next((s for s in config["subscriptions"] if s["id"] == sub_id), None)
    if not sub or subs_status.get(sub_id, {}).get("running") or not sub.get("enabled", True):
        return
    start_ts = time.time()
    use_hw = config["settings"]["use_hwaccel"]
    res_filter = [r.lower() for r in sub.get("res_filter", ["sd", "720p", "1080p", "4k", "8k"])]
    subs_status[sub_id] = {
        "running": True,
        "stop_requested": False,
        "total": 0,
        "current": 0,
        "success": 0,
        "sub_name": sub['name'],
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
        r = requests.get(sub["url"], timeout=15, verify=False)
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

        with ThreadPoolExecutor(max_workers=int(sub.get("threads", 10))) as executor:
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
    # 新增统计
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

    # 存档状态（包含 valid_list）
    arch = {
        "update_time": update_ts,
        "duration": duration,
        "logs": status["logs"],
        "stats": {
            "total": status["total"],
            "current": status["current"],
            "success": status["success"],
            "banned": len(status["blacklisted_hosts"])
        },
        "analytics": status["analytics"],
        "valid_list": valid_list
    }
    with open(os.path.join(OUTPUT_DIR, f"last_status_{sub_id}.json"), "w", encoding="utf-8") as f:
        json.dump(arch, f, ensure_ascii=False)

    # 输出 M3U 和 TXT
    try:
        m3u_p = os.path.join(OUTPUT_DIR, f"{sub_id}.m3u")
        txt_p = os.path.join(OUTPUT_DIR, f"{sub_id}.txt")
        epg = config["settings"]["epg_url"]
        logo = config["settings"]["logo_base"]
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
    config = load_config()
    for agg in config.get("aggregates", []):
        if sub_id in agg.get("subscription_ids", []):
            threading.Thread(target=run_aggregate, args=(agg["id"],), kwargs={"auto": True}).start()

# ---------- 聚合任务（增强版，支持分组和每个频道多个链接）----------
def run_aggregate(agg_id, auto=False):
    # 防止同一聚合任务并发运行
    if aggregates_status.get(agg_id, {}).get("running"):
        return
    aggregates_status[agg_id] = {"running": True, "logs": []}
    
    def log(msg):
        ts = get_now()
        aggregates_status[agg_id]["logs"].append(f"{ts} - {msg}")
    
    log(f"🚀 聚合任务开始 (自动: {auto})")
    config = load_config()
    agg = next((a for a in config.get("aggregates", []) if a["id"] == agg_id), None)
    if not agg:
        log("❌ 聚合配置不存在")
        aggregates_status[agg_id]["running"] = False
        return

    log(f"📋 聚合名称: {agg['name']}")
    log(f"📦 包含订阅: {', '.join(agg.get('subscription_ids', []))}")

    # 读取所有选中订阅的 last_status 文件，收集有效频道
    channel_map = {}  # 标准名 -> [频道信息列表]
    total_channels = 0
    for sub_id in agg.get("subscription_ids", []):
        status_path = os.path.join(OUTPUT_DIR, f"last_status_{sub_id}.json")
        if not os.path.exists(status_path):
            log(f"⚠️ 订阅 {sub_id} 状态文件不存在，跳过")
            continue
        with open(status_path, 'r', encoding='utf-8') as f:
            status = json.load(f)
        valid_list = status.get("valid_list", [])
        log(f"📡 订阅 {sub_id} 提供了 {len(valid_list)} 个有效源")
        for item in valid_list:
            std_name, matched = match_channel_name(item["name"])
            if matched:
                log(f"🔤 别名匹配: '{item['name']}' -> '{std_name}'")
            else:
                log(f"🔤 未匹配别名: '{item['name']}' 保持原样")
            item_copy = item.copy()
            item_copy["name"] = std_name
            if std_name not in channel_map:
                channel_map[std_name] = []
            channel_map[std_name].append(item_copy)
            total_channels += 1

    log(f"📊 共收集到 {total_channels} 个原始频道，去重后 {len(channel_map)} 个标准频道")

    # 对每个频道的列表按评分降序排序
    for name in channel_map:
        channel_map[name].sort(key=lambda x: x['score'], reverse=True)

    # 读取 demo.txt 获取顺序和分组信息
    ordered_names = []
    group_map = {}  # 标准名 -> 分组名称
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
        ordered_names = sorted(channel_map.keys())
        log(f"📋 未找到 demo.txt，使用字母顺序")

    # 按顺序生成最终列表（展平所有频道的所有链接）
    final_list = []
    matched_count = 0
    for name in ordered_names:
        if name in channel_map:
            for item in channel_map[name]:
                item["group"] = group_map.get(name, "未分组")
                final_list.append(item)
                matched_count += 1
        else:
            log(f"⚠️ demo.txt 中的频道 '{name}' 在源中未找到")

    log(f"✅ 最终生成 {len(final_list)} 个有效链接")

    # 确定使用的 EPG URL
    epg_url = config["settings"]["epg_url"]  # 默认
    epg_agg_id = agg.get("epg_aggregate_id")
    if epg_agg_id:
        epg_agg = next((e for e in config.get("epg_aggregates", []) if e["id"] == epg_agg_id), None)
        if epg_agg:
            epg_url = f"{request.host_url.rstrip('/')}/epg/{epg_agg_id}.xml"
            log(f"📺 使用 EPG 聚合: {epg_agg['name']} -> {epg_url}")
        else:
            log(f"⚠️ 指定的 EPG 聚合不存在，使用全局 EPG")
    else:
        log(f"📺 使用全局 EPG: {epg_url}")

    # 生成输出文件
    update_ts = get_now()
    logo_base = config["settings"]["logo_base"]
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

    # 记录聚合状态
    agg_status = {
        "update_time": update_ts,
        "total": len(final_list),
        "subscriptions": agg["subscription_ids"],
        "files": {
            "m3u": f"/aggregate/{agg_id}.m3u",
            "txt": f"/aggregate/{agg_id}.txt"
        }
    }
    agg_status_path = os.path.join(OUTPUT_DIR, f"aggregate_{agg_id}_status.json")
    with open(agg_status_path, 'w', encoding='utf-8') as f:
        json.dump(agg_status, f, ensure_ascii=False)

    log(f"🏁 聚合任务完成，耗时 {format_duration(time.time() - start_time)}")
    aggregates_status[agg_id]["running"] = False

# ---------- EPG 聚合（增强版：自动解压 gzip、错误恢复）----------
def run_epg_aggregate(epg_agg_id, auto=False):
    if epg_aggregates_status.get(epg_agg_id, {}).get("running"):
        return
    epg_aggregates_status[epg_agg_id] = {"running": True, "logs": []}
    
    def log(msg):
        ts = get_now()
        epg_aggregates_status[epg_agg_id]["logs"].append(f"{ts} - {msg}")
    
    log(f"📺 EPG 聚合任务开始 (自动: {auto})")
    config = load_config()
    epg_agg = next((e for e in config.get("epg_aggregates", []) if e["id"] == epg_agg_id), None)
    if not epg_agg:
        log("❌ EPG 聚合配置不存在")
        epg_aggregates_status[epg_agg_id]["running"] = False
        return

    log(f"📋 EPG 聚合名称: {epg_agg['name']}")
    log(f"🔗 源列表: {', '.join(epg_agg['sources'])}")
    cache_days = epg_agg.get("cache_days", 3)
    log(f"📅 缓存天数: {cache_days}")

    # 计算需要的日期范围
    today = datetime.date.today()
    date_list = [today + datetime.timedelta(days=i) for i in range(-1, cache_days)]  # 前一天到 cache_days-1 天后
    date_strs = [d.strftime('%Y%m%d') for d in date_list]
    log(f"📅 需要包含的日期: {', '.join(date_strs)}")

    # 存储所有节目的字典，键为 (channel, start, title) 用于去重
    programmes = {}

    # 下载并解析每个源
    for idx, source_url in enumerate(epg_agg['sources']):
        log(f"⬇️ 正在下载源 {idx+1}: {source_url}")
        try:
            resp = requests.get(source_url, timeout=30)
            if resp.status_code != 200:
                log(f"⚠️ 源 {source_url} 返回状态码 {resp.status_code}，跳过")
                continue
            content = resp.content

            # 处理可能为 gzip 压缩的内容（根据 URL 后缀或 Content-Encoding 头部）
            is_gz = source_url.endswith('.gz') or resp.headers.get('Content-Encoding') == 'gzip'
            if is_gz:
                try:
                    # 尝试解压
                    buf = BytesIO(content)
                    with gzip.GzipFile(fileobj=buf) as gz_file:
                        content = gz_file.read()
                    log(f"📦 检测到 gzip 压缩，已解压")
                except Exception as e:
                    log(f"⚠️ 解压失败: {str(e)}，尝试直接解析")

            # 尝试解析 XML（使用 recover 模式容错）
            try:
                parser = ET.XMLParser(recover=True)
                tree = ET.parse(BytesIO(content), parser=parser)
                root = tree.getroot()
            except Exception as e:
                log(f"❌ 解析 XML 失败: {str(e)}")
                continue

            # 遍历所有 programme
            count = 0
            for prog in root.findall('programme'):
                start = prog.get('start')
                channel = prog.get('channel')
                title_elem = prog.find('title')
                title = title_elem.text if title_elem is not None else ''
                # 检查日期是否在范围内
                if start and len(start) >= 8:
                    prog_date = start[:8]  # YYYYMMDD
                    if prog_date in date_strs:
                        key = (channel, start, title)
                        if key not in programmes:
                            programmes[key] = prog
                            count += 1
            log(f"➕ 源 {idx+1} 添加了 {count} 个节目")
        except Exception as e:
            log(f"❌ 下载源 {source_url} 失败: {str(e)}")

    log(f"📊 共收集到 {len(programmes)} 个节目")

    # 构建新的 XML
    new_root = ET.Element('tv')
    # 将所有节目添加到新树
    for prog in programmes.values():
        new_root.append(prog)

    # 生成 XML 文件
    update_ts = get_now()
    xml_path = os.path.join(OUTPUT_DIR, f"epg_{epg_agg_id}.xml")
    gz_path = os.path.join(OUTPUT_DIR, f"epg_{epg_agg_id}.xml.gz")
    
    # 写入 XML
    tree = ET.ElementTree(new_root)
    tree.write(xml_path, encoding='utf-8', xml_declaration=True)

    # 写入 GZ
    with open(xml_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            f_out.writelines(f_in)

    log(f"💾 XML 已保存: {xml_path}")
    log(f"💾 GZ 已保存: {gz_path}")

    # 记录状态
    epg_status = {
        "update_time": update_ts,
        "total": len(programmes),
        "sources": epg_agg['sources'],
        "files": {
            "xml": f"/epg/{epg_agg_id}.xml",
            "gz": f"/epg/{epg_agg_id}.xml.gz"
        }
    }
    status_path = os.path.join(OUTPUT_DIR, f"epg_{epg_agg_id}_status.json")
    with open(status_path, 'w', encoding='utf-8') as f:
        json.dump(epg_status, f, ensure_ascii=False)

    log(f"🏁 EPG 聚合任务完成")
    epg_aggregates_status[epg_agg_id]["running"] = False

# ---------- 计划任务调度 ----------
def clear_sub_jobs(sub_id):
    for job in scheduler.get_jobs():
        if job.id.startswith(sub_id):
            scheduler.remove_job(job.id)

def schedule_subscription(sub):
    sub_id = sub["id"]
    clear_sub_jobs(sub_id)
    if not sub.get("enabled", True):
        return
    mode = sub.get("schedule_mode", "none")
    if mode == "none":
        return
    elif mode == "fixed":
        times = sub.get("fixed_times", "").split(",")
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
        hours = int(sub.get("interval_hours", 1))
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
    config = load_config()
    for sub in config["subscriptions"]:
        schedule_subscription(sub)

# ---------- EPG 聚合任务调度 ----------
def clear_epg_jobs(epg_agg_id):
    for job in scheduler.get_jobs():
        if job.id.startswith(f"epg_{epg_agg_id}"):
            scheduler.remove_job(job.id)

def schedule_epg_aggregation(epg_agg):
    epg_id = epg_agg["id"]
    clear_epg_jobs(epg_id)
    if not epg_agg.get("enabled", True):
        return
    interval = int(epg_agg.get("update_interval", 24))
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
    config = load_config()
    for epg_agg in config.get("epg_aggregates", []):
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
    config = load_config()
    if request.method == 'POST':
        new_sub = request.json
        if not new_sub.get("id"):
            new_sub["id"] = str(uuid.uuid4())[:8]
            config["subscriptions"].append(new_sub)
        else:
            for i, s in enumerate(config["subscriptions"]):
                if s["id"] == new_sub["id"]:
                    config["subscriptions"][i] = new_sub
        save_config(config)
        return jsonify({"status": "ok"})
    return jsonify({"subs": config["subscriptions"], "settings": config["settings"]})

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
    config = load_config()
    config["settings"] = request.json
    save_config(config)
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
    config = load_config()
    config["subscriptions"] = [s for s in config["subscriptions"] if s["id"] != sub_id]
    save_config(config)
    clear_sub_jobs(sub_id)
    return jsonify({"status": "ok"})

@app.route('/sub/<sub_id>.<ext>')
def get_sub_file(sub_id, ext):
    return send_from_directory(OUTPUT_DIR, f"{sub_id}.{ext}")

# ---------- 聚合相关 API ----------
@app.route('/api/aggregates', methods=['GET', 'POST'])
def api_aggregates():
    config = load_config()
    if request.method == 'POST':
        data = request.json
        agg_list = config.get("aggregates", [])
        if not data.get("id"):
            data["id"] = str(uuid.uuid4())[:8]
            agg_list.append(data)
        else:
            for i, a in enumerate(agg_list):
                if a["id"] == data["id"]:
                    agg_list[i] = data
        config["aggregates"] = agg_list
        save_config(config)
        return jsonify({"status": "ok"})
    else:
        agg_list = config.get("aggregates", [])
        result = []
        for agg in agg_list:
            status_path = os.path.join(OUTPUT_DIR, f"aggregate_{agg['id']}_status.json")
            last_update = "从未"
            if os.path.exists(status_path):
                try:
                    with open(status_path, 'r', encoding='utf-8') as f:
                        st = json.load(f)
                        last_update = st.get("update_time", "从未")
                except:
                    pass
            agg_copy = agg.copy()
            agg_copy["last_update"] = last_update
            result.append(agg_copy)
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
    config = load_config()
    agg_list = config.get("aggregates", [])
    config["aggregates"] = [a for a in agg_list if a["id"] != agg_id]
    save_config(config)
    return jsonify({"status": "ok"})

@app.route('/aggregate/<agg_id>.<ext>')
def get_aggregate_file(agg_id, ext):
    return send_from_directory(OUTPUT_DIR, f"aggregate_{agg_id}.{ext}")

# ---------- EPG 聚合相关 API ----------
@app.route('/api/epg_aggregates', methods=['GET', 'POST'])
def api_epg_aggregates():
    config = load_config()
    if request.method == 'POST':
        data = request.json
        epg_list = config.get("epg_aggregates", [])
        if not data.get("id"):
            data["id"] = str(uuid.uuid4())[:8]
            epg_list.append(data)
        else:
            for i, e in enumerate(epg_list):
                if e["id"] == data["id"]:
                    epg_list[i] = data
        config["epg_aggregates"] = epg_list
        save_config(config)
        return jsonify({"status": "ok"})
    else:
        epg_list = config.get("epg_aggregates", [])
        result = []
        for epg in epg_list:
            status_path = os.path.join(OUTPUT_DIR, f"epg_{epg['id']}_status.json")
            last_update = "从未"
            if os.path.exists(status_path):
                try:
                    with open(status_path, 'r', encoding='utf-8') as f:
                        st = json.load(f)
                        last_update = st.get("update_time", "从未")
                except:
                    pass
            epg_copy = epg.copy()
            epg_copy["last_update"] = last_update
            result.append(epg_copy)
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
    config = load_config()
    epg_list = config.get("epg_aggregates", [])
    config["epg_aggregates"] = [e for e in epg_list if e["id"] != epg_id]
    save_config(config)
    return jsonify({"status": "ok"})

# ---------- EPG 文件路由（支持 .xml 和 .xml.gz）----------
@app.route('/epg/<epg_id>.xml')
def get_epg_xml(epg_id):
    filename = f"epg_{epg_id}.xml"
    return send_from_directory(OUTPUT_DIR, filename)

@app.route('/epg/<epg_id>.xml.gz')
def get_epg_xml_gz(epg_id):
    filename = f"epg_{epg_id}.xml.gz"
    return send_from_directory(OUTPUT_DIR, filename)

# 保留旧版 .gz 路由，重定向到 .xml.gz 以兼容旧链接
@app.route('/epg/<epg_id>.gz')
def get_epg_gz_redirect(epg_id):
    return redirect(f"/epg/{epg_id}.xml.gz", code=301)

# ---------- 启动时初始化调度 ----------
with app.app_context():
    reschedule_all()
    reschedule_epg_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5123)
