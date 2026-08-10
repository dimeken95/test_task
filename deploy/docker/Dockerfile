# syntax=docker/dockerfile:1

# ---- build ----------------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies are their own layer so source edits do not re-download them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# -trimpath keeps build paths out of the binary; the ldflags stamp the build so
# /version and payload_build_info can report exactly what is running.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags="-s -w \
        -X github.com/dimeken95/test_task/internal/buildinfo.Version=${VERSION} \
        -X github.com/dimeken95/test_task/internal/buildinfo.Commit=${COMMIT}" \
      -o /out/server ./cmd/server

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/mockprocessor ./cmd/mockprocessor

# ---- runtime --------------------------------------------------------------
# distroless/static: no shell, no package manager, nothing to exploit beyond
# our own binary. The build is CGO-free so nothing else is needed.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app
COPY --from=build /out/server /app/server
COPY --from=build /out/mockprocessor /app/mockprocessor
COPY migrations /app/migrations

USER nonroot:nonroot
EXPOSE 8080 8090

ENTRYPOINT ["/app/server"]
