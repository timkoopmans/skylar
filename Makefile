.PHONY: list
SHELL := /bin/bash
export DOCKER_BUILDKIT=1

check:
	cargo check --workspace

list:
	@awk -F: '/^[A-z]/ {print $$1}' Makefile | sort

build:
	cargo build
	docker compose build

release:
	cargo build --release
	docker compose build

push:
	docker compose push

up:
	docker compose up --detach --force-recreate scylla

down:
	docker compose down --remove-orphans

reset:
	docker compose down --remove-orphans scylla
	docker compose up --detach --force-recreate scylla
