#!/usr/bin/env bash
set -euo pipefail

if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  echo "Refusing to continue: .env is tracked by Git." >&2
  exit 1
fi

python3 <<'PY'
from pathlib import Path
import re
import subprocess
import sys

patterns = {
    "private key": re.compile(r"BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY"),
    "GitHub token": re.compile(r"(?:gh" + r"p_|github_pat_)[A-Za-z0-9_]{20,}"),
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
}

violations = []
for name in subprocess.check_output(["git", "ls-files", "-z"]).split(b"\0"):
    if not name:
        continue
    path = Path(name.decode())
    try:
        content = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        continue
    for label, pattern in patterns.items():
        if pattern.search(content):
            violations.append(f"{path}: possible {label}")

if violations:
    print("\n".join(violations), file=sys.stderr)
    sys.exit(1)
PY

python3 <<'PY'
import json
import subprocess
import sys

command = [
    "docker", "compose", "--profile", "analytics", "--profile", "monitoring",
    "--profile", "airflow", "--profile", "batch", "--profile", "loadtest",
    "config", "--format", "json",
]
config = json.loads(subprocess.check_output(command))
violations = []
for service_name, service in config.get("services", {}).items():
    image = service.get("image", "")
    if image.endswith(":latest") or (image and ":" not in image.rsplit("/", 1)[-1]):
        violations.append(f"{service_name}: image is not version-pinned ({image})")
    for port in service.get("ports", []) or []:
        if not port.get("published"):
            continue
        host_ip = port.get("host_ip")
        if host_ip != "127.0.0.1":
            violations.append(
                f"{service_name}: published port {port.get('published')} is bound to {host_ip or 'all interfaces'}"
            )

if violations:
    print("\n".join(violations), file=sys.stderr)
    sys.exit(1)
PY

while IFS= read -r dockerfile; do
  if grep -E '^FROM[[:space:]]+[^[:space:]]+:latest([[:space:]]|$)' "$dockerfile" >/dev/null; then
    echo "$dockerfile: contains an unpinned latest base image" >&2
    exit 1
  fi
done < <(git ls-files 'Dockerfile*' '**/Dockerfile*')

echo "Security baseline passed: no tracked .env/high-confidence secret, no latest image and every published Compose port is loopback-only."
