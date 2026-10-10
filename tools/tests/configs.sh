#!/bin/bash

FAIL=0

/usr/local/bin/xray run -test -confdir /etc/xray/conf || FAIL=1
/usr/local/bin/sing-box check -C /etc/singbox/conf || FAIL=1
haproxy -c -f /run/cfg/connector/haproxy/haproxy.cfg || FAIL=1
envoy --mode validate -c /run/cfg/connector/envoy/envoy.yaml || FAIL=1
nginx -t -c /run/cfg/connector/nginx/nginx.conf || FAIL=1
caddy validate --config /run/cfg/connector/caddy/Caddyfile --adapter caddyfile || FAIL=1

exit "$FAIL"
