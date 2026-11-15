#!/usr/bin/env bash
set -euo pipefail

pm2 delete cme-exchange-info-summary
pm2 save
