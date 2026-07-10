.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help setup build run attach logs monitor watch export stop down status purge

help:  ## Show available commands
	@echo "Hourly Futures Backfill — available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  make %-9s %s\n", $$1, $$2}'

setup:  ## Install Docker+Compose+screen+make and add swap (run once)
	@bash setup.sh

build:  ## Build the backfill image
	docker compose build

run:  ## Start MongoDB + launch the backfill in a detached screen session
	docker compose up -d mongo
	@screen -dmS hourly bash -c 'docker compose run --rm backfill 2>&1 | tee -a backfill.log'
	@echo ""
	@echo "Backfill started in screen session 'hourly'."
	@echo "  make logs       # follow progress"
	@echo "  make monitor    # DB snapshot"
	@echo "  make watch      # auto-refresh every 30s"
	@echo "  screen -r hourly  # attach to live session"

attach:  ## Attach to the live screen session
	screen -r hourly

logs:  ## Follow the backfill log
	@touch backfill.log && tail -f backfill.log

monitor:  ## One-time snapshot of ingest progress
	@bash monitor.sh

watch:  ## Live-refresh progress every 30s
	watch -n 30 'bash monitor.sh'

export:  ## Dump dataset to a portable .archive.gz
	@./export_data.sh

stop:  ## Stop MongoDB (data on disk is kept)
	docker compose stop

down:  ## Stop and remove containers (data in ./data/mongo is kept)
	docker compose down

status:  ## Show container status
	docker compose ps

purge:  ## DANGER: remove containers AND delete all data
	docker compose down -v
	sudo rm -rf ./data
