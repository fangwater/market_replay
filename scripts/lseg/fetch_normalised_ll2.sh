#!/usr/bin/env bash
# Downloads normalisedLL2 daily packages (trial; not part of lseg-daily).
#   fetch_normalised_ll2.sh <products-file|P1,P2,..> <YYYY-MM-DD,YYYY-MM-DD,...>
# Same rules as lseg-daily for the TAS packages: read-only GET without proxy,
# at most 2 transfers at a time, zero-byte placeholders are skipped, a file whose
# Last-Modified is younger than SETTLE_MINUTES (default 60) is not settled yet and
# is skipped, an existing local file is never overwritten (size must match the
# remote Content-Length, otherwise it is reported), data goes to <name>.partial
# and is renamed only after the size matched.
set -euo pipefail
BASE="${REMOTE_BASE:-http://10.61.20.12/public/users/u158/public/lseg_data/future_daily/normalisedLL2}"
DEST="${RAW_DEST:-/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2}"
SETTLE="${SETTLE_MINUTES:-60}"
[[ $# -eq 2 ]] || { echo "usage: $0 <products-file|P1,P2> <day1,day2,..>" >&2; exit 2; }
if [[ -f "$1" ]]; then products=$(tr ',\n' '  ' <"$1"); else products=${1//,/ }; fi
days=${2//,/ }
one() {
  local p=$1 d=$2 name url dir out head len mod age
  name="${p}_${d}_normll2.csv.gz"; url="$BASE/$p/$name"; dir="$DEST/$p"; out="$dir/$name"
  head=$(curl -sS --noproxy '*' -I --max-time 60 "$url") || { echo "ERR   $name head failed"; return 0; }
  grep -q ' 200' <<<"${head%%$'\n'*}" || { echo "MISS  $name (no remote file)"; return 0; }
  len=$(awk 'tolower($1)=="content-length:"{print $2+0}' <<<"$head")
  mod=$(awk 'tolower($1)=="last-modified:"{sub(/^[^:]*: */,""); print}' <<<"$head" | tr -d '\r')
  if [[ "${len:-0}" -eq 0 ]]; then echo "ZERO  $name (zero-byte placeholder)"; return 0; fi
  age=$(( ($(date +%s) - $(date -d "$mod" +%s)) / 60 ))
  if (( age < SETTLE )); then echo "WAIT  $name (modified ${age} min ago)"; return 0; fi
  if [[ -e "$out" ]]; then
    if [[ "$(stat -c %s "$out")" == "$len" ]]; then echo "HAVE  $name"; else echo "DIFF  $name local size differs from remote, not overwritten"; fi
    return 0
  fi
  mkdir -p "$dir"
  curl -sS --noproxy '*' --fail --max-time 7200 -o "$out.partial" "$url" || { rm -f "$out.partial"; echo "ERR   $name download failed"; return 0; }
  if [[ "$(stat -c %s "$out.partial")" != "$len" ]]; then rm -f "$out.partial"; echo "ERR   $name size mismatch"; return 0; fi
  mv "$out.partial" "$out"; echo "GOT   $name $len"
}
export -f one; export BASE DEST SETTLE
for p in $products; do for d in $days; do printf '%s %s\n' "$p" "$d"; done; done | xargs -P 2 -L 1 bash -c 'one "$0" "$1"'
