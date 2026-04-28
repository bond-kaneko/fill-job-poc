.PHONY: up down logs test test-river test-sqs test-custom tidy

up:
	docker compose up -d
	@echo "waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U poc >/dev/null 2>&1; do sleep 0.5; done
	@echo "ready."

down:
	docker compose down -v

logs:
	docker compose logs -f

tidy:
	cd shared && go mod tidy
	cd river-impl && go mod tidy
	cd sqs-impl && go mod tidy
	cd custom-impl && go mod tidy

test: test-custom test-river test-sqs

test-custom:
	cd custom-impl && go test ./... -count=1 -timeout 120s

test-river:
	cd river-impl && go test ./... -count=1 -timeout 120s

test-sqs:
	cd sqs-impl && go test ./... -count=1 -timeout 120s

load: load-custom load-river load-sqs

load-custom:
	cd custom-impl && go test -tags=load ./... -run TestLoadThroughput -v -count=1 -timeout 300s

load-river:
	cd river-impl && go test -tags=load ./... -run TestLoadThroughput -v -count=1 -timeout 300s

load-sqs:
	cd sqs-impl && go test -tags=load ./... -run TestLoadThroughput -v -count=1 -timeout 300s
