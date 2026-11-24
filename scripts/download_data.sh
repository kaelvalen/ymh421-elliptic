#!/usr/bin/env bash
# Elliptic++ veri setini (≈1.6 GB) data/raw/ altına indirir ve SHA-256 ile doğrular.
# Kaynak: https://github.com/git-disl/EllipticPlusPlus (Git LFS)
set -euo pipefail

cd "$(dirname "$0")/.."
DEST=data/raw
BASE="https://media.githubusercontent.com/media/git-disl/EllipticPlusPlus/main"
mkdir -p "$DEST"

# dosya | repo içi yol | sha256 (Git LFS oid)
FILES=(
  "txs_features.csv|Transactions%20Dataset|2db326ec8ddb68f1d810c1834e1ff62e0a8300378f0984a1e3b2ca82a439821b"
  "txs_classes.csv|Transactions%20Dataset|013a11742969071a906878ded0319571df0657f9b7133e5c6cdb36217bf0d240"
  "txs_edgelist.csv|Transactions%20Dataset|a35053ba68a98e4382cae2ba65b9d9e36b23b6439e02dff084971b1b72a5156e"
  "wallets_features.csv|Actors%20Dataset|317daca2810c355ddfdb8c0dab34cf11d1aa90567fe975090c7e5a901386eb77"
  "wallets_classes.csv|Actors%20Dataset|4e5132c99f941666bf1fefd4100a1428d339c9252ec6987909e1adf8eac902f9"
  "AddrAddr_edgelist.csv|Actors%20Dataset|ffba894458e262a691e5e4d006f5dc1d0e069fabfe828f443fd157bf7f8393f2"
  "AddrTx_edgelist.csv|Actors%20Dataset|f5f903f752387f66a1bccaeff54e293b2e8470fcddf5eb56b88aa06fd23a8f3b"
  "TxAddr_edgelist.csv|Actors%20Dataset|9f5afbdde7bc3d91fb7a4655be55799d6504cd0063ae55a0753a5a41189932b8"
)

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

for entry in "${FILES[@]}"; do
  IFS='|' read -r name folder hash <<< "$entry"
  out="$DEST/$name"
  if [[ -f "$out" && "$(sha "$out")" == "$hash" ]]; then
    echo "✓ $name (zaten var)"; continue
  fi
  echo "↓ $name"
  curl -L --fail --retry 3 --progress-bar -o "$out" "$BASE/$folder/$name"
  if [[ "$(sha "$out")" != "$hash" ]]; then
    echo "✗ $name: SHA-256 uyuşmuyor. GitHub LFS kotası dolmuş olabilir; README'deki Google Drive yolunu kullanın." >&2
    exit 1
  fi
  echo "✓ $name"
done
echo "Tamam: $(du -sh "$DEST" | cut -f1) → $DEST"
