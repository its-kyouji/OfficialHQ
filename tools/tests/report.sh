#!/bin/bash

FAIL="$1"
HTTP1="$2"
HTTP2="$3"

printf "\n%-20s | %-10s\n" "LANE / TEST" "RESULT"
printf -- "---------------------+------------\n"

printf "%-20s | %-10s\n" \
    "Pre-flight Configs" \
    "$([ "$FAIL" -eq 0 ] && echo PASS || echo FAIL)"

printf "%-20s | %-10s\n" \
    "HTTP/1.1 (/health)" \
    "$([ "$HTTP1" = "200" ] && echo PASS || echo FAIL)"

printf "%-20s | %-10s\n" \
    "h2c gRPC (/health)" \
    "$([ "$HTTP2" = "200" ] && echo PASS || echo FAIL)"

echo -e "\n==== UNVERIFIED ITEMS ===="
echo "1. gRPC serviceName path shape: Verify if proxies accept -grpc or -grpc/Tun."
echo "   Fix: Test with real client. Adjust HAProxy regex if needed."
echo "2. XHTTP wire paths downgrade: Verify if XHTTP works over downgraded HTTP/1.1."
echo "   Fix: Monitor Cloud Run logs during XHTTP client connection."
echo "3. Cleartext h1 to h2c auto-upgrade in HAProxy 2.x on Debian Bookworm."
echo "   Fix: Test 'curl --http2-prior-knowledge' against HAProxy port directly."
echo "4. OVPN Bridge setup: Lacks remote VM OpenVPN target integration."
echo "   Fix: Provision external VM and inject details to bridge-ovpn.json."
echo "5. Sing-box array merge order: Verify if chain.json loads first."
echo "   Fix: sing-box merge /tmp/out.json -C /etc/singbox/conf && cat /tmp/out.json"
