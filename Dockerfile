FROM python:3.9-slim

# 1. 开启 Debian 的 contrib, non-free 和 non-free-firmware 软件源
# 使用更通用的方式处理新版 Debian 的 .sources 格式和旧版的 .list 格式
RUN set -ex; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i 's/Components: main/Components: main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources; \
    else \
        sed -i 's/main$/main contrib non-free non-free-firmware/g' /etc/apt/sources.list; \
    fi

# 2. 安装 QSV 和 VAAPI 完整驱动链
# 适配 Trixie/Bookworm: 使用 intel-media-va-driver-non-free 和 libvpl2 (替代旧版 libmfx)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libva-drm2 \
    libva2 \
    i965-va-driver \
    intel-media-va-driver-non-free \
    mesa-va-drivers \
    libvpl2 \
    vainfo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5123
CMD ["python", "-u", "app.py"]
