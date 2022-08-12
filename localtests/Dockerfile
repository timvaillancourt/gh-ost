FROM golang:1.18
LABEL maintainer="github@github.com"

RUN apt-get update && apt-get install -y default-mysql-client

COPY . /go/src/github.com/github/gh-ost
WORKDIR /go/src/github.com/github/gh-ost

RUN go build -o bin/gh-ost go/cmd/gh-ost/main.go

ENTRYPOINT ["./localtests/test.sh", "-b", "bin/gh-ost"]
