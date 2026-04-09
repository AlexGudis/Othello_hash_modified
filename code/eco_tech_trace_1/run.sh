#!/usr/bin/env bash
set -euo pipefail

# =========================
# Configuration
# =========================
START_IDX="046"
END_IDX="046"
FNAME_PREFIX="dump_2025-07-02_01-41-04.pcap"
SHARE_URL_PREFIX="https://cloud.asvk.cs.msu.ru/s/3QobqzkfJfWZGLy/download?path=%2F&files="

TEST1_BIN="./test1"
TEST1_SRC="./test1.cpp"
PLOT_SCRIPT="./plot_results.py"
RESULTS_DIR="results_map"
OUT_DIR="out"
FORCE_REPROCESS="${FORCE_REPROCESS:-0}"
SHOW_CURL_PROGRESS="${SHOW_CURL_PROGRESS:-1}"

# =========================
# Helpers
# =========================
need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    exit 1
  }
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

curl_fetch() {
  local url="$1"
  if [[ "$SHOW_CURL_PROGRESS" == "1" ]]; then
    curl --fail -L --progress-bar "$url"
  else
    curl --fail -L --silent --show-error "$url"
  fi
}

# =========================
# Dependency checks
# =========================
need_cmd curl
need_cmd zstd
need_cmd python3
need_cmd awk
need_cmd sort

mkdir -p "$RESULTS_DIR" "$OUT_DIR"

# =========================
# Build test1 if needed
# =========================
if [[ ! -x "$TEST1_BIN" ]]; then
  if [[ -f "$TEST1_SRC" ]]; then
    if command -v c++ >/dev/null 2>&1; then
      CXX="c++"
    elif command -v g++ >/dev/null 2>&1; then
      CXX="g++"
    else
      echo "ERROR: no C++ compiler found (need c++ or g++)" >&2
      exit 1
    fi

    log "Building $TEST1_BIN from $TEST1_SRC"
    "$CXX" -O3 -std=c++20 "$TEST1_SRC" -lpcap -o "$TEST1_BIN"
  else
    echo "ERROR: $TEST1_BIN not found and $TEST1_SRC not found." >&2
    echo "       Put compiled binary ./test1 рядом со скриптом или исходник ./test1.cpp" >&2
    exit 1
  fi
fi

if [[ ! -f "$PLOT_SCRIPT" ]]; then
  echo "ERROR: plot script not found: $PLOT_SCRIPT" >&2
  exit 1
fi

# =========================
# Normalize each PCAP chunk into packet events
# Output format per line:
#   ts_us<TAB>key_u64<TAB>dst_port
# where key_u64 = (dst_mac << 12) | vlan
# =========================
log "Processing PCAP chunks from ${START_IDX} to ${END_IDX}"
for i in $(seq -w "$START_IDX" "$END_IDX"); do
  fname="${FNAME_PREFIX}${i}.zst"
  out_zst="$RESULTS_DIR/${fname%.zst}.events.tsv.zst"
  tmp_zst="$out_zst.tmp"

  if [[ -s "$out_zst" && "$FORCE_REPROCESS" != "1" ]]; then
    log "Skipping $fname (already normalized: $out_zst)"
    continue
  fi

  rm -f "$tmp_zst"
  log "Downloading and normalizing $fname"
  curl_fetch "${SHARE_URL_PREFIX}${fname}" \
    | zstd -dc \
    | "$TEST1_BIN" \
    | zstd -T0 -q -o "$tmp_zst"

  mv "$tmp_zst" "$out_zst"
  log "Saved $out_zst"
done

# =========================
# Build all summaries and plots from normalized events
# =========================
log "Building summaries and plots"
python3 "$PLOT_SCRIPT" "$OUT_DIR" "$RESULTS_DIR"

log "Done"
log "Main outputs:"
log "  $OUT_DIR/summary_metrics.tsv"
log "  $OUT_DIR/key_summary.tsv"
log "  $OUT_DIR/vlan_summary.tsv"
log "  $OUT_DIR/new_keys_per_sec.tsv"
log "  $OUT_DIR/churn_per_sec.tsv"
log "  $OUT_DIR/sizing_summary.tsv"
log "Plots:"
log "  $OUT_DIR/plots/"