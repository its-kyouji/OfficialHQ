# ==============================
# STAGE 1: BUILDER
# ==============================
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential wget git cmake zlib1g-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ==== DROPBEAR BUILD ====
WORKDIR /build
RUN wget --no-check-certificate https://matt.ucc.asn.au/dropbear/releases/dropbear-2024.85.tar.bz2 && \
    tar -xjf dropbear-2024.85.tar.bz2

WORKDIR /build/dropbear-2024.85
RUN ./configure --disable-zlib --disable-syslog && make && make install

# ==== BADVPN BUILD ====
WORKDIR /build
RUN git clone --depth 1 https://github.com/ambrop72/badvpn.git

WORKDIR /build/badvpn
RUN mkdir build && cd build && \
    cmake .. -DBUILD_NOTHING_BY_DEFAULT=1 -DBUILD_UDPGW=1 && \
    make install

# ==============================
# STAGE 2: RUNTIME
# ==============================
FROM debian:bookworm-slim

ENV XRAY_VERSION="26.7.28"
ENV SINGBOX_VERSION="1.14.2"
ENV DEBIAN_FRONTEND="noninteractive"

# ==== SYSTEM DEPENDENCIES ====
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip python3 python3-uvloop nginx haproxy caddy ca-certificates openssh-server \
    debian-keyring debian-archive-keyring apt-transport-https netcat-openbsd gnupg \
    supervisor \
    && curl -sL 'https://apt.envoyproxy.io/signing.key' | gpg --dearmor -o /usr/share/keyrings/envoy-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/envoy-keyring.gpg] https://apt.envoyproxy.io bookworm main" > /etc/apt/sources.list.d/envoy.list \
    && apt-get update && apt-get install -y envoy \
    && rm -rf /var/lib/apt/lists/*

# ==== XRAY INSTALLATION ====
RUN curl -sSLf "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" -o xray.zip \
    && unzip xray.zip -d /usr/local/bin/ xray \
    && rm xray.zip && chmod +x /usr/local/bin/xray \
    && mkdir -p /usr/local/share/xray

# ==== SING-BOX INSTALLATION ====
RUN curl -sSLf "https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-amd64.tar.gz" -o singbox.tar.gz \
    && tar -xzf singbox.tar.gz --strip-components=1 -C /usr/local/bin/ sing-box-${SINGBOX_VERSION}-linux-amd64/sing-box \
    && rm singbox.tar.gz && chmod +x /usr/local/bin/sing-box

# ==== BUILT BINARY TRANSFER ====
COPY --from=builder /usr/local/sbin/dropbear /usr/local/sbin/
COPY --from=builder /usr/local/bin/dropbearkey /usr/local/bin/
COPY --from=builder /usr/local/bin/badvpn-udpgw /usr/local/bin/

# ==== XRAY GEO DATA ====
RUN curl -sSL "https://github.com/v2fly/geoip/releases/latest/download/geoip.dat" -o /usr/local/share/xray/geoip.dat

# ==============================
# APPLICATION WORKSPACE
# ==============================
WORKDIR /app
COPY . /app

RUN chmod +x \
    /app/startup/entrypoint.sh \
    /app/startup/wait-for.sh \
    /app/tools/*.sh \
    /app/tools/tests/*.sh

# ==============================
# RUNTIME DIRECTORIES
# ==============================
RUN mkdir -p \
    /var/log/supervisor \
    /run/sshd \
    /etc/xray/conf \
    /etc/singbox/conf \
    /run/cfg/connector \
    /run/cfg/relay

EXPOSE 8080

ENTRYPOINT ["/app/startup/entrypoint.sh"]
