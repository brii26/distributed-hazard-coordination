#!/usr/bin/env bash
# Greps the report for patterns we don't allow. Pass files, or it checks every chapter.
cd "$(dirname "$0")" || exit 1
files=("$@")
[ ${#files[@]} -eq 0 ] && files=(chapters/*.tex backmatter/*.tex)
bad=0
check() {
  if grep -nEi "$2" "${files[@]}"; then echo "^^ $1"; bad=1; fi
}
check "kalimat pembuka generik" 'di era (globalisasi|digital)|seiring (dengan )?(pesatnya|perkembangan)|tidak dapat dipungkiri'
check "terjemahan dalam tanda kurung" '\\textit\{[^}]*\} ?\(|\([^)]*\\textit\{'
check "jangan menulis zero trust" 'zero[ -]trust'
check "placeholder tersisa" '<placeholder>|\\textless\{?\}? ?placeholder'
exit $bad
