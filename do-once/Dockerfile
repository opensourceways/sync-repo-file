FROM golang:latest as BUILDER

MAINTAINER zengchen1024<chenzeng765@gmail.com>

# build binary
COPY . /go/src/github.com/opensourceways/sync-repo-file
RUN cd /go/src/github.com/opensourceways/sync-repo-file && CGO_ENABLED=1 go build -v -o ./sync-repo-file ./do-once

# copy binary config and utils
FROM golang:latest
RUN  mkdir -p /opt/app/
# overwrite config yaml
COPY  --from=BUILDER /go/src/github.com/opensourceways/sync-repo-file/sync-repo-file /opt/app

WORKDIR /opt/app/
ENTRYPOINT ["/opt/app/sync-repo-file"]
