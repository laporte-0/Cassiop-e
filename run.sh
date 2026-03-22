#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"
REQ_FILE="$SCRIPT_DIR/requirements.txt"
PYTHON_BIN="$VENV_DIR/bin/python"
PIP_BIN="$VENV_DIR/bin/pip"
SCRAPER="$SCRIPT_DIR/crawl_posts_with_scrapy.py"
PLAYWRIGHT_SCRAPER="$SCRIPT_DIR/crawl_posts_with_playwright.py"

DEFAULT_INPUT="$SCRIPT_DIR/working_links.txt"
DEFAULT_TEMPLATE="$SCRIPT_DIR/Fichier de données.xlsx"
DEFAULT_SOURCE="$SCRIPT_DIR/Cassiopée Envoi2 Cactus à CryptOn.xlsx"
DEFAULT_OUTPUT="$SCRIPT_DIR/resultats_posts_scraped.xlsx"
DEFAULT_PROXY="http://127.0.0.1:8118"
DEFAULT_BROWSER_PROXY="socks5://127.0.0.1:9050"

INPUT_FILE="${INPUT_FILE:-$DEFAULT_INPUT}"
TEMPLATE_FILE="${TEMPLATE_FILE:-$DEFAULT_TEMPLATE}"
SOURCE_FILE="${SOURCE_FILE:-$DEFAULT_SOURCE}"
OUTPUT_FILE="${OUTPUT_FILE:-$DEFAULT_OUTPUT}"
TOR_HTTP_PROXY="${TOR_HTTP_PROXY:-$DEFAULT_PROXY}"
BROWSER_PROXY="${BROWSER_PROXY:-$DEFAULT_BROWSER_PROXY}"
CONCURRENCY="${CONCURRENCY:-8}"
TIMEOUT="${TIMEOUT:-20}"
MODE="${MODE:-raw}"
ENGINE="${ENGINE:-scrapy}"
INTERACTIVE=false

print_help() {
  cat <<'EOF'
Usage:
  ./run.sh
  ./run.sh -i
  ./run.sh --input links.txt --output result.xlsx

Short options:
  -i, --interactive      Ask questions instead of typing long commands
  -n, --input FILE       Input links file (.txt/.csv/.xlsx)
  -o, --output FILE      Output Excel file
  -p, --proxy URL        HTTP proxy for onion pages (default: http://127.0.0.1:8118)
  -e, --engine NAME      scrapy | playwright (default: scrapy)
      --browser-proxy    Browser proxy for Playwright (default: socks5://127.0.0.1:9050)
  -t, --timeout SEC      Request timeout (default: 20)
  -c, --concurrency N    Scrapy concurrency (default: 8)
  -m, --mode MODE        raw | mapped | both (default: raw)
      --template FILE    Template file (default: Fichier de données.xlsx)
      --source FILE      Source context file (default: Cassiopée Envoi2 Cactus à CryptOn.xlsx)
  -h, --help             Show this help

Examples:
  ./run.sh
  ./run.sh -m both
  ./run.sh -e playwright -m raw
  ./run.sh -i
  ./run.sh -n working_links.txt -o resultats_posts_scraped.xlsx
  TOR_HTTP_PROXY="http://127.0.0.1:8118" ./run.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--interactive)
      INTERACTIVE=true
      shift
      ;;
    -n|--input)
      INPUT_FILE="$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    -p|--proxy)
      TOR_HTTP_PROXY="$2"
      shift 2
      ;;
    -e|--engine)
      ENGINE="$2"
      shift 2
      ;;
    -t|--timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    -c|--concurrency)
      CONCURRENCY="$2"
      shift 2
      ;;
    -m|--mode)
      MODE="$2"
      shift 2
      ;;
    --template)
      TEMPLATE_FILE="$2"
      shift 2
      ;;
    --source)
      SOURCE_FILE="$2"
      shift 2
      ;;
    --browser-proxy)
      BROWSER_PROXY="$2"
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_help
      exit 1
      ;;
  esac
done

if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

"$PIP_BIN" install -q --upgrade pip
"$PIP_BIN" install -q -r "$REQ_FILE"

if [[ "$INTERACTIVE" == true ]]; then
  read -r -p "Input links file [$INPUT_FILE]: " answer
  INPUT_FILE="${answer:-$INPUT_FILE}"

  read -r -p "Output Excel file [$OUTPUT_FILE]: " answer
  OUTPUT_FILE="${answer:-$OUTPUT_FILE}"

  read -r -p "HTTP proxy for onion pages [$TOR_HTTP_PROXY]: " answer
  TOR_HTTP_PROXY="${answer:-$TOR_HTTP_PROXY}"

  read -r -p "Engine (scrapy|playwright) [$ENGINE]: " answer
  ENGINE="${answer:-$ENGINE}"

  read -r -p "Browser proxy for Playwright [$BROWSER_PROXY]: " answer
  BROWSER_PROXY="${answer:-$BROWSER_PROXY}"

  read -r -p "Timeout seconds [$TIMEOUT]: " answer
  TIMEOUT="${answer:-$TIMEOUT}"

  read -r -p "Concurrency [$CONCURRENCY]: " answer
  CONCURRENCY="${answer:-$CONCURRENCY}"

  read -r -p "Mode (raw|mapped|both) [$MODE]: " answer
  MODE="${answer:-$MODE}"
fi

if [[ "$MODE" != "raw" && "$MODE" != "mapped" && "$MODE" != "both" ]]; then
  echo "Invalid mode: $MODE (expected raw, mapped, or both)" >&2
  exit 1
fi

if [[ "$ENGINE" != "scrapy" && "$ENGINE" != "playwright" ]]; then
  echo "Invalid engine: $ENGINE (expected scrapy or playwright)" >&2
  exit 1
fi

if [[ ! -f "$SCRAPER" ]]; then
  echo "Missing scraper file: $SCRAPER" >&2
  exit 1
fi

if [[ ! -f "$PLAYWRIGHT_SCRAPER" ]]; then
  echo "Missing scraper file: $PLAYWRIGHT_SCRAPER" >&2
  exit 1
fi

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if [[ ! -f "$TEMPLATE_FILE" ]]; then
  echo "Template file not found: $TEMPLATE_FILE" >&2
  exit 1
fi

has_onion=false
if grep -qi "\.onion" "$INPUT_FILE"; then
  has_onion=true
fi

if [[ "$has_onion" == true && "$ENGINE" == "scrapy" ]]; then
  if ! "$PYTHON_BIN" - <<'PY' "$TOR_HTTP_PROXY"
import socket
import sys
from urllib.parse import urlparse

proxy = sys.argv[1]
try:
    parsed = urlparse(proxy)
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        raise ValueError("invalid proxy")
    with socket.create_connection((host, port), timeout=3):
        pass
except Exception:
    sys.exit(1)
sys.exit(0)
PY
  then
    echo "Detected .onion links but proxy is unreachable: $TOR_HTTP_PROXY" >&2
    echo "Start Tor + Privoxy first, then retry:" >&2
    echo "  sudo apt install -y tor privoxy" >&2
    echo "  sudo systemctl start tor privoxy" >&2
    exit 1
  fi
fi

if [[ "$ENGINE" == "playwright" ]]; then
  "$PIP_BIN" install -q playwright
  "$PYTHON_BIN" -m playwright install chromium >/dev/null 2>&1 || true
fi

echo "Running scraper..."
echo "  Input: $INPUT_FILE"
echo "  Output: $OUTPUT_FILE"
echo "  Mode: $MODE"
echo "  Engine: $ENGINE"
echo "  Proxy: $TOR_HTTP_PROXY"
echo "  Browser proxy: $BROWSER_PROXY"

if [[ "$ENGINE" == "playwright" ]]; then
  exec "$PYTHON_BIN" "$PLAYWRIGHT_SCRAPER" \
    --input "$INPUT_FILE" \
    --template-file "$TEMPLATE_FILE" \
    --source-file "$SOURCE_FILE" \
    --browser-proxy "$BROWSER_PROXY" \
    --timeout "$TIMEOUT" \
    --mode "$MODE" \
    --output "$OUTPUT_FILE"
else
  exec "$PYTHON_BIN" "$SCRAPER" \
    --input "$INPUT_FILE" \
    --template-file "$TEMPLATE_FILE" \
    --source-file "$SOURCE_FILE" \
    --tor-proxy "$TOR_HTTP_PROXY" \
    --timeout "$TIMEOUT" \
    --concurrency "$CONCURRENCY" \
    --mode "$MODE" \
    --output "$OUTPUT_FILE"
fi
