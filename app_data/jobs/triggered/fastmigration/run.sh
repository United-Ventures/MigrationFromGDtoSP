#!/usr/bin/env bash
set -e

echo "🔧 Installing dependencies..."
pip install -r requirements.txt

echo "🚀 Launching migration..."
python fastMigrationV8.py          