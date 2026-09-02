FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod *.go ./
RUN go test ./... && CGO_ENABLED=0 go build -o /concurrent-counter .

FROM scratch
COPY --from=build /concurrent-counter /concurrent-counter
ENTRYPOINT ["/concurrent-counter"]
