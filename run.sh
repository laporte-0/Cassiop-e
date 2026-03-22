#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"
REQ_FILE="$SCRIPT_DIR/requirements.txt"
PYTHON_BIN="$VENV_DIR/bin/python"
PIP_BIN="$VENV_DIR/bin/pip"

if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

"$PIP_BIN" install -q --upgrade pip
"$PIP_BIN" install -q -r "$REQ_FILE"

if [[ -z "${TOR_PROXY:-}" ]]; then
  export TOR_PROXY="socks5h://127.0.0.1:9050"
fi

has_tor_proxy_arg=false
has_no_default_arg=false
for arg in "$@"; do
  if [[ "$arg" == "--tor-proxy" ]]; then
    has_tor_proxy_arg=true
  fi
  if [[ "$arg" == "--no-default-tor-proxy" ]]; then
    has_no_default_arg=true
  fi
done

if [[ "$has_tor_proxy_arg" == true || "$has_no_default_arg" == true ]]; then
  exec "$PYTHON_BIN" "$SCRIPT_DIR/process_darkweb_links.py" "$@"
else
  exec "$PYTHON_BIN" "$SCRIPT_DIR/process_darkweb_links.py" --tor-proxy "$TOR_PROXY" "$@"
fi
