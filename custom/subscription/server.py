#!/usr/bin/env python3

import socketserver

from config import PORT
from handler import SubHandler
# =================
# SERVER ENTRYPOINT
# =================
with socketserver.TCPServer(("", PORT), SubHandler) as httpd:
    print(f"Sub server listening on port {PORT}", flush=True)
    httpd.serve_forever()
