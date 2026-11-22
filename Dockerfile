FROM golang:1.24 AS build
WORKDIR /src
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /jobq ./cmd/jobq

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /jobq /jobq
EXPOSE 8080
ENTRYPOINT ["/jobq"]
