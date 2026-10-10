# Sing-Box Tuning Guide
- **Vanilla Limitation:** Sing-box does not support XHTTP (`xh`) transport natively in core builds; traffic is restricted to WebSocket and HTTPUpgrade.
- **Multiplexing:** Uses native `tcp_fast_open` and automatic interface detection.
