.PHONY: up down worker api celery test tidy

up:          ## start Temporal + Redis locally
	docker compose up -d

down:
	docker compose down

tidy:        ## resolve Go modules (run once on your Mac)
	cd go && go mod tidy

test:        ## Go workflow tests (no server needed)
	cd go && go test ./...

worker:      ## run the Temporal worker in dry-run mode
	cd go && DRY_RUN=1 go run ./cmd/worker

worker-real: ## run the worker against real nodes
	cd go && DRY_RUN=0 K3S_URL=$(K3S_URL) go run ./cmd/worker

api:         ## run the Flask API on :8080
	cd python && FLASK_APP=api flask run -p 8080

celery:      ## run the Celery worker
	cd python && celery -A tasks.celery worker --loglevel=info
