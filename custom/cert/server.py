#!/usr/bin/env python3

import http.server
import socketserver

from config import PORT
# ==================
# CERTIFICATE SERVER
# ==================
Handler = http.server.SimpleHTTPRequestHandler

print(
    f"[CERT SERVER] Stub active on port {PORT}. "
    "(UNVERIFIED: Needs remote VM integration)",
    flush=True
)

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
