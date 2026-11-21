#!/usr/bin/env bash
set -euo pipefail
cd /workspace
go version
printf 'Linux CI-equivalent unit test package count: '
wc -l < /checks/packages-linux.txt
mapfile -t packages < /checks/packages-linux.txt
go test -p 1 -race "${packages[@]}"
