REPO := github.com/juanfresia/claud
BIN_DIR := bin

all:
	go build -o $(BIN_DIR)/claud "$(REPO)/cmd/claud"
.PHONY: all

clean:
	rm -rf $(BIN_DIR)
.PHONY: clean

.DEFAULT: all
