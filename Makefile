.PHONY: infra infra-destroy up down run psql logs teardown

# ── Infrastructure ────────────────────────────────────────────────────────────

infra:
	cd terraform && terraform init && terraform apply -auto-approve

infra-destroy:
	cd terraform && terraform destroy -auto-approve

# ── Docker ────────────────────────────────────────────────────────────────────

up:
	docker compose up --build -d

down:
	docker compose down

# ── Pipeline ──────────────────────────────────────────────────────────────────

run:
	docker compose exec ingester set -a && source .env && set +a && GOOGLE_APPLICATION_CREDENTIALS=./keys/my-creds.json uv run python main.py 2>&1

# ── Debugging ─────────────────────────────────────────────────────────────────

psql:
	docker compose exec db psql -U $${POSTGRES_USER} -d $${POSTGRES_DB}

logs:
	docker compose logs -f ingester

# ── Full teardown ─────────────────────────────────────────────────────────────

teardown: infra-destroy down
	docker compose down -v