client
dev tun
proto tcp
remote __EXIT_IP__ __EXIT_PORT__
resolv-retry infinite
nobind
persist-key
persist-tun
auth-user-pass
cipher AES-256-GCM
# TLS/CA details to be injected via cert_server.py
