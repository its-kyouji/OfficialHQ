#!/bin/bash

echo -e "\n[SELFTEST] Running smoke tests..."

HTTP1=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/health)

HTTP2=$(curl --http2-prior-knowledge -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/health 2>/dev/null)
