#!/usr/bin/env bash
# Downloads LSEG normalisedLL2 supplement packages (future_supplement/normalisedLL2/<job>/merged-Data*.csv.gz and
# merged-Report.csv.gz); not part of lseg-daily.
#   fetch_supplement_ll2.sh <job-dir> [<job-dir> ...]
# Same rules as fetch_normalised_ll2.sh: read-only GET without proxy, at most 2 transfers at a time, a file younger than
# SETTLE_MINUTES (default 60) is skipped, an existing local file is never overwritten (size must match the remote
# Content-Length, otherwise it is reported), data goes to <name>.partial and is renamed only after the size matched.
set -euo pipefail
BASE="${REMOTE_BASE:-http://10.61.20.12/public/users/u158/public/lseg_data/future_supplement/normalisedLL2}"
DEST="${RAW_DEST:-/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_supplement/normalisedLL2}"
SETTLE="${SETTLE_MINUTES:-60}"
[[ $# -ge 1 ]] || { echo "usage: $0 <job-dir> [<job-dir> ...]" >&2; exit 2; }
one() {
  local job=${1%/} name=$2 url dir out head len mod age
  url="$BASE/$job/$name"; dir="$DEST/$job"; out="$dir/$name"
  head=$(curl -sS --noproxy '*' -I --max-time 60 "$url") || { echo "ERR   $job/$name head failed"; return 0; }
  grep -q ' 200' <<<"${head%%$'\n'*}" || { echo "MISS  $job/$name (no remote file)"; return 0; }
  len=$(awk 'tolower($1)=="content-length:"{print $2+0}' <<<"$head")
  mod=$(awk 'tolower($1)=="last-modified:"{sub(/^[^:]*: */,""); print}' <<<"$head" | tr -d '\r')
  if [[ "${len:-0}" -eq 0 ]]; then echo "ZERO  $job/$name (zero-byte placeholder)"; return 0; fi
  age=$(( ($(date +%s) - $(date -d "$mod" +%s)) / 60 ))
  if (( age < SETTLE )); then echo "WAIT  $job/$name (modified ${age} min ago)"; return 0; fi
  if [[ -e "$out" ]]; then
    if [[ "$(stat -c %s "$out")" == "$len" ]]; then echo "HAVE  $job/$name"; else echo "DIFF  $job/$name local size differs from remote, not overwritten"; fi
    return 0
  fi
  mkdir -p "$dir"
  curl -sS --noproxy '*' --fail --max-time 14400 -o "$out.partial" "$url" || { rm -f "$out.partial"; echo "ERR   $job/$name download failed"; return 0; }
  if [[ "$(stat -c %s "$out.partial")" != "$len" ]]; then rm -f "$out.partial"; echo "ERR   $job/$name size mismatch"; return 0; fi
  mv "$out.partial" "$out"; echo "GOT   $job/$name $len"
}
export -f one; export BASE DEST SETTLE
for job in "$@"; do
  for name in merged-Report.csv.gz merged-Data.csv.gz; do printf '%s %s\n' "$job" "$name"; done
done | xargs -P 2 -L 1 bash -c 'one "$0" "$1"'
