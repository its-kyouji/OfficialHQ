# Xray-Core Tuning Guide
- **TCP Congestion Control:** Enabled via sockopt (`bbr`).
- **Keep-Alive:** Maintained at 15-second intervals to prevent carrier NAT timeout termination.
- **Sniffing:** Active on all inbounds to support ad-blocking and domain-based routing without breaking destination overrides.
