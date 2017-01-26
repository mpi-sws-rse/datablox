# compile the message sequence charts

all: block_initialization.svg

.pic.svg:
	pic2plot -Tsvg $< >$@

.SUFFIXES: .pic .svg

.PHONY: all
