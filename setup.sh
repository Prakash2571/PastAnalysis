#!/usr/bin/env bash
#
# One-shot setup for a fresh instance.
# Installs Docker + Compose + screen + make, adds swap.
#
set -euo pipefail

echo "==> [1/3] Ensuring 2G swap..."
if sudo swapon --show 2>/dev/null | grep -q '/swapfile'; then
  echo "    swap already active, skipping."
else
  if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile 2>/dev/null || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
  fi
  sudo swapon /swapfile
  grep -q '/swapfile' /etc/fstab 2>/dev/null || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
  echo "    swap enabled."
fi

echo "==> [2/3] Installing Docker + Compose plugin..."
if command -v docker >/dev/null 2>&1; then
  echo "    docker already installed, skipping."
else
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER" 2>/dev/null || true

echo "==> [3/3] Installing screen + make..."
if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq screen make
elif command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y -q screen make
elif command -v yum >/dev/null 2>&1; then
  sudo yum install -y -q screen make
fi

echo
echo "=================================================================="
echo " Setup complete."
echo
echo " IMPORTANT: log out and back in (or run: newgrp docker)"
echo "            so you can use docker without sudo."
echo
echo " Then start the backfill with:"
echo "     make run"
echo "=================================================================="
