# syntax=docker/dockerfile:1.7

# 1) Build the web panel (Vite -> internal/webui/dist).
FROM node:24-alpine AS web
RUN corepack enable
WORKDIR /src/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm build

# 2) Build the Go single binary (CGO disabled; static).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Overlay the freshly built web assets over the committed placeholder.
COPY --from=web /src/internal/webui/dist ./internal/webui/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /katrix ./cmd/katrix

# 3) Runtime image (alpine, non-root).
FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S katrix && adduser -S -G katrix katrix
COPY --from=build /katrix /usr/local/bin/katrix
USER katrix
EXPOSE 8008 8448
HEALTHCHECK --interval=30s --timeout=5s CMD ["katrix", "healthcheck"]
ENTRYPOINT ["katrix"]
