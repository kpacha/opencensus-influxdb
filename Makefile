BIN_NAME :=krakend
DEP_VERSION=0.4.1
OS := $(shell uname | tr '[:upper:]' '[:lower:]')
VERSION :=0.4.2

all: deps test build

prepare:
	@echo "Installing dep..."
	@curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-${OS}-amd64 -o ${GOPATH}/bin/dep
	@chmod a+x ${GOPATH}/bin/dep

deps:
	@echo "Setting up the vendors folder..."
	@dep ensure -v
	@echo ""
	@echo "Resolved dependencies:"
	@dep status
	@echo ""

test:
	go test -v -cover

build:
	@go build ./example