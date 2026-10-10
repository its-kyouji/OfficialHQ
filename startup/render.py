#!/usr/bin/env python3

import os
import re
import sys

# === TEMPLATE TOKEN DEFINITIONS ===

tokens = {
    "__UUID__": os.environ.get("UUID", ""),
    "__IN_PASS__": os.environ.get("IN_PASS", ""),
    "__IN_SS__": os.environ.get("IN_SS", ""),
    "__BASE_PATH__": os.environ.get("BASE_PATH", ""),
    "__BASE_SVC__": os.environ.get("BASE_PATH", "").lstrip("/"),
    "__EXIT_IP__": os.environ.get("EXIT_IP", ""),
    "__EXIT_PASS__": os.environ.get("EXIT_PASS", ""),
    "__EXIT_PATH__": os.environ.get("EXIT_PATH", ""),
    "__EXIT_SEC__": os.environ.get("EXIT_SEC", ""),
    "__EXIT_SS__": os.environ.get("EXIT_SS", ""),
    "PREFIX_PLACEHOLDER": os.environ.get("PATH_PREFIX", ""),
    "SUFFIX_PLACEHOLDER": os.environ.get("SUFFIX", ""),
}

numeric_port = os.environ.get("EXIT_PORT", "443")

# === JSONC COMMENT STRIPPING ===
def strip_jsonc(text):
    return re.sub(
        r'("(?:\\.|[^"\\])*")|//.*?$|/\*.*?\*/',
        lambda match: match.group(1) if match.group(1) else "",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )


# === RUNTIME CONFIGURATION RENDERING ===
dirs_to_process = [
    "/etc/xray/conf",
    "/etc/singbox/conf",
    "/run/cfg",
]

print("[INIT] Processing JSON substitution & stripping...")

for directory in dirs_to_process:
    for root, _, files in os.walk(directory):
        for filename in files:
            path = os.path.join(root, filename)

            with open(path, "r") as file:
                content = file.read()

            if path.endswith(".json"):
                content = strip_jsonc(content)

            content = content.replace(
                '"__EXIT_PORT__"',
                numeric_port,
            )

            for token, value in tokens.items():
                content = content.replace(token, value)

            leftovers = re.findall(r"__[A-Z_]+__", content)

            if leftovers:
                print(
                    f"[FATAL] Leftover token(s) "
                    f"{leftovers} found in {path}"
                )
                sys.exit(1)

            with open(path, "w") as file:
                file.write(content)

print("[INIT] Template rendering complete.")
