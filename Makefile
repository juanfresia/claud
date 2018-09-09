REPO := github.com/juanfresia/claud
BIN_DIR := bin

all:
	go build -o $(BIN_DIR)/claud "$(REPO)/cmd/claud"
.PHONY: all

docker:
	mkdir -p bin
	sudo docker build -t claud-dev:latest -f Dockerfile .
	sudo docker run -v "${PWD}/bin:/build" claud-dev:latest 'cp /claud/bin/* /build/'

clean:
	rm -rf $(BIN_DIR)
.PHONY: clean

.DEFAULT: all
