#!/usr/bin/env bash
set -e

echo "==> Installation des dependances Python"
pip install -r requirements.txt

echo "==> Installation de Chromium pour Playwright"
playwright install chromium

echo "==> Build termine"
