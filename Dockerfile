# 使用 Debian bookworm 基础镜像以获得更新的驱动包
FROM python:3.9-slim-bookworm

# 启用 contrib、non-free 和 non-free-firmware 软件源（兼容新旧格式）
RUN set -ex; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i 's/Components: main/Components: main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources; \
    else \
        sed -i 's/main$/main contrib non-free non-free-firmware/g' /etc/apt/sources.list; \
    fi

# 安装 ffmpeg、VAAPI/QSV 驱动、vainfo 等必要组件
RUN apt-get update && apt-get install -y \
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

WORKDIR /app

# 复制依赖文件并安装 Python 包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

EXPOSE 5123

# 使用 -u 参数确保 Python 日志实时输出
CMD ["python", "-u", "app.py"]
