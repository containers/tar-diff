PROJECT := tar-diff

VERSION := $(shell grep -oP 'VERSION\s*=\s*"\K[^"]+' pkg/protocol/version.go)

PROJ_TARBALL := $(PROJECT)_$(VERSION).tar.gz
GOCOVERDIR := $(CURDIR)/test/coverage


.PHONY: all build clean fmt install lint test tools dist unit-test integration-test validate .install.golangci-lint

export GOPROXY=https://proxy.golang.org


GOBIN := $(shell go env GOBIN)
ifeq ($(GOBIN),)
GOPATH := $(shell go env GOPATH)
GOBIN := $(GOPATH)/bin
endif

GOFLAGS:=
ifeq ($(GOFLAGS),)
GOFLAGS := -buildvcs=false
endif

PACKAGES := $(shell go list $(GOFLAGS) ./...)
SOURCE_DIRS = $(shell echo $(PACKAGES) | awk 'BEGIN{FS="/"; RS=" "}{print $$4}' | uniq)

PREFIX ?= /usr
INSTALLDIR=${DESTDIR}${PREFIX}/bin

export PATH := $(PATH):${GOBIN}

all: tools tar-diff tar-patch test validate

$(PROJ_TARBALL):
	git archive --prefix=tar-diff_$(VERSION)/ --format=tar.gz HEAD --output $(PROJ_TARBALL) 

build:
	mkdir -p $(GOCOVERDIR)
	go build $(GOFLAGS) -cover -o tar-diff ./cmd/tar-diff
	go build $(GOFLAGS) -cover -o tar-patch ./cmd/tar-patch

tar-diff:
	go build $(GOFLAGS) ./cmd/tar-diff

tar-patch:
	go build $(GOFLAGS) ./cmd/tar-patch

install: tar-diff tar-patch
	install -d -m 755 ${INSTALLDIR}
	install -m 755 tar-diff ${INSTALLDIR}/tar-diff
	install -m 755 tar-patch ${INSTALLDIR}/tar-patch

tools: .install.golangci-lint

.install.golangci-lint:
	if [ ! -x "$(GOBIN)/golangci-lint" ]; then \
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/main/install.sh | sh -s -- -b $(GOBIN) v2.10.1; \
	fi

clean:
	rm -f tar-diff tar-patch
	rm -rf $(PROJECT)_$(VERSION) $(PROJ_TARBALL)
	rm -rf $(dir $(GOCOVERDIR))

integration-test: build
	GOCOVERDIR=$(GOCOVERDIR) tests/test.sh
	GOCOVERDIR=$(GOCOVERDIR) tests/test-multi-old.sh
	GOCOVERDIR=$(GOCOVERDIR) tests/test-source-prefix.sh
	GOCOVERDIR=$(GOCOVERDIR) tests/test-delta-paths.sh
	GOCOVERDIR=$(GOCOVERDIR) tests/test-tar-errors.sh
	GOCOVERDIR=$(GOCOVERDIR) tests/test-fuzzy-abs.sh
	go tool covdata percent -i=$(GOCOVERDIR) -o=$(GOCOVERDIR)/integration.out


unit-test:
	mkdir -p $(GOCOVERDIR)
	go test $(GOFLAGS) -coverprofile=$(GOCOVERDIR)/unit.out ./...
	go tool cover -func=$(GOCOVERDIR)/unit.out

test: unit-test integration-test

fmt:
	@gofmt -l -s -w $(SOURCE_DIRS)

validate: lint
	@go vet $(GOFLAGS) ./...
	@if [ -n "$$WINDIR" ]; then echo "Skipping gofmt check on Windows"; else output=$$(gofmt -s -l .); test -z "$$output" || (echo "$$output"; exit 1); fi

lint:
	GOFLAGS=$(GOFLAGS) $(GOBIN)/golangci-lint run

dist: $(PROJ_TARBALL)
	@echo "Created $(PROJ_TARBALL)"
