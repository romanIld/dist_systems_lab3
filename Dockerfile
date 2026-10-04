# Builds the cluster node and the client on Linux and packs them into a small runtime image.

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go vet ./... \
 && CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/raftnode ./cmd/client

FROM alpine:3.22
WORKDIR /app
COPY --from=build /out/ /app/bin/
ENV PATH="/app/bin:${PATH}"
EXPOSE 8080
ENTRYPOINT ["raftnode"]
