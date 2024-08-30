# Stage 1: Build the Go binary
FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.21 AS builder

# Set the target architecture as a build argument
ARG TARGETOS
ARG TARGETARCH

# Set the Go environment for cross-compilation
ENV CGO_ENABLED=0
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}

# Set the working directory inside the container
WORKDIR /app

# Initialize a new Go module if go.mod does not exist
RUN [ ! -f go.mod ] && go mod init github.com/sciclon2/kafka-lag-go || true

# Copy the rest of the application code
COPY . .

# Ensure the Go module is up to date
RUN go mod tidy

# Build the Go binary for the main application
RUN go build -o /out/kafka-lag-go ./cmd/kafka-lag-go

# Stage 2: Create the final runtime image
FROM --platform=${BUILDPLATFORM:-linux/amd64} debian:bullseye-slim

# Install necessary packages including CA certificates
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /out/kafka-lag-go /usr/local/bin/kafka-lag-go

# Command to run the Go application
ENTRYPOINT ["/usr/local/bin/kafka-lag-go"]
