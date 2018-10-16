VERSION := 0.1-alpha

# git commit info
COMMIT := $(shell git rev-parse --short HEAD)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

REPO := github.com/juanfresia/claud
BIN_DIR := bin

BUILD_FLAGS := -X $(REPO)/claud/cmd.version=$(VERSION)
BUILD_FLAGS += -X $(REPO)/claud/cmd.commit=$(COMMIT)
BUILD_FLAGS += -X $(REPO)/claud/cmd.branch=$(BRANCH)
BUILD_FLAGS := -ldflags "$(BUILD_FLAGS)" -v

all: fmt
	go build $(BUILD_FLAGS) -o $(BIN_DIR)/claud "$(REPO)/claud"
.PHONY: all

fmt:
	go fmt ./...
	go vet ./...
.PHONY: fmt

docker:
	mkdir -p bin
	sudo docker build -t claud-dev:latest -f Dockerfile .
	sudo docker run --net=host -v "${PWD}/bin:/build" claud-dev:latest 'cd /claud; make; cp /claud/bin/* /build/'
.PHONY: docker

clean:
	rm -rf $(BIN_DIR) *.log
.PHONY: clean

.DEFAULT: all
