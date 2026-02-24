# 使用 Debian bookworm 基础镜像以获得更新的驱动包
FROM python:3.11-slim-bookworm

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai \
    DEBUG_HW=0

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 启用 contrib、non-free 和 non-free-firmware 软件源（兼容新旧格式）
RUN set -ex; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i 's/Components: main/Components: main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources; \
    else \
        sed -i 's/main$/main contrib non-free non-free-firmware/g' /etc/apt/sources.list; \
    fi

# 安装 ffmpeg、VAAPI/QSV 驱动、vainfo 等必要组件，并清理缓存
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libva-drm2 \
    libva-x11-2 \
    libva-glx2 \
    i965-va-driver \
    intel-media-va-driver-non-free \
    mesa-va-drivers \
    libvpl2 \
    libmfx1 \
    vainfo \
    && rm -rf /var/lib/apt/lists/*

# 创建应用用户（UID 1000），并设置工作目录权限
# 注：如果需要以非 root 运行，请取消下面行的注释，并在 docker-compose 中设置 user: "1000:1000"
# 同时确保数据目录属主为 1000，或修改权限
# RUN groupadd -g 1000 app && useradd -u 1000 -g app -m -s /bin/bash app
# RUN mkdir -p /app/data && chown -R app:app /app

WORKDIR /app

# 复制依赖文件并安装 Python 包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建数据目录（如果不存在）
RUN mkdir -p /app/data/log /app/data/output

# 暴露端口
EXPOSE 5123

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:5123/api/sys_info', timeout=3)" || exit 1

# 启动命令
CMD ["python", "-u", "app.py"]
