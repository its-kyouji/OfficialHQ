#!/bin/bash

FAIL=0

echo -e "\n[SELFTEST] Checking listening ports..."

for port in 8080 8081 8082 8083 8084 10001 10005 10009 10013 11001 11004 11007 2200 2201 2222 2223; do
    if nc -z 127.0.0.1 "$port"; then
        echo "Port $port: UP"
    else
        echo "Port $port: DOWN"
        FAIL=1
    fi
done

exit "$FAIL"
