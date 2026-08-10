.PHONY: help build test test-integration test-e2e cover lint fmt tidy check \
        run compose-up compose-down compose-scale compose-logs \
        k8s-deploy k8s-delete docker-build smoke clean

VERSION    ?= dev
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
IMAGE      ?= payload-service:$(VERSION)
COMPOSE    := docker compose -f deploy/compose/docker-compose.yml
LDFLAGS    := -X github.com/dimeken95/test_task/internal/buildinfo.Version=$(VERSION) \
              -X github.com/dimeken95/test_task/internal/buildinfo.Commit=$(COMMIT)

# Spun up on demand by `make test-integration`.
PG_CONTAINER ?= payload-test-pg
PG_PORT      ?= 55432
TEST_DSN     ?= postgres://test:test@localhost:$(PG_PORT)/test?sslmode=disable

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---- build ----------------------------------------------------------------

build: ## Build both binaries into ./bin
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/server ./cmd/server
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/mockprocessor ./cmd/mockprocessor

docker-build: ## Build the container image
	docker build -f deploy/docker/Dockerfile \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		-t $(IMAGE) -t payload-service:latest .

run: build ## Run the server locally (expects deps to be up)
	./bin/server

## ---- quality --------------------------------------------------------------

check: fmt-check tidy-check lint test ## Everything CI runs

test: ## Unit tests with the race detector
	go test -race ./...

test-integration: ## Full suite against a throwaway Postgres (needs Docker)
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true
	@docker run -d --rm --name $(PG_CONTAINER) \
		-e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
		-p $(PG_PORT):5432 postgres:16-alpine >/dev/null
	@echo "waiting for postgres..."
	@for i in $$(seq 1 30); do \
		docker exec $(PG_CONTAINER) pg_isready -U test -d test >/dev/null 2>&1 && break; \
		sleep 1; \
	done
	@TEST_DATABASE_URL="$(TEST_DSN)" go test -race -count=1 ./... ; status=$$? ; \
		docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 ; exit $$status

test-e2e: ## End-to-end pipeline tests only (needs TEST_DATABASE_URL)
	go test -race -count=1 -v ./test/e2e/...

cover: ## Coverage report
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	@echo "run: go tool cover -html=coverage.out"

lint: ## golangci-lint (falls back to go vet if not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, running go vet"; go vet ./...; \
	fi

fmt: ## Format the tree
	gofmt -w .

fmt-check: ## Fail if anything is unformatted
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "not gofmt-ed:"; echo "$$unformatted"; exit 1; fi

tidy: ## Tidy modules
	go mod tidy

tidy-check: ## Fail if go.mod/go.sum are not tidy
	@go mod tidy && git diff --exit-code go.mod go.sum

## ---- local stack ----------------------------------------------------------

compose-up: ## Start the full stack
	$(COMPOSE) up -d --build
	@echo
	@echo "API        http://localhost:8080   (nginx -> api replicas)"
	@echo "Grafana    http://localhost:3000   (anonymous admin)"
	@echo "Prometheus http://localhost:9090"
	@echo "Jaeger     http://localhost:16686"
	@echo "MinIO      http://localhost:9001   (minioadmin / minioadmin)"

compose-scale: ## Scale out: make compose-scale API=3 WORKERS=4
	$(COMPOSE) up -d --scale api=$(or $(API),3) --scale worker=$(or $(WORKERS),4)
	$(COMPOSE) ps

compose-logs: ## Tail application logs
	$(COMPOSE) logs -f api worker

compose-down: ## Stop the stack and drop volumes
	$(COMPOSE) down -v

smoke: ## Submit a job through the running stack and poll it to completion
	@./scripts/smoke.sh

## ---- kubernetes -----------------------------------------------------------

k8s-deploy: docker-build ## Apply all manifests (expects a local cluster)
	kubectl apply -f deploy/k8s/
	kubectl -n payload rollout status deploy/payload-api --timeout=180s
	kubectl -n payload rollout status deploy/payload-worker --timeout=180s

k8s-delete: ## Remove everything
	kubectl delete -f deploy/k8s/ --ignore-not-found

clean:
	rm -rf bin coverage.out
