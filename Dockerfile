FROM golang:1.26.3-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the latent binary
RUN CGO_ENABLED=0 GOOS=linux go build -o latent ./cmd/latent/main.go

FROM alpine:latest
RUN apk update
RUN apk --no-cache add ca-certificates netcat-openbsd

WORKDIR /root/
COPY --from=builder /app/latent .

# Start the application
CMD ["./latent"]
