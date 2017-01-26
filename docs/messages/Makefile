# compile the message sequence charts

all: messages.html

messages.html: messages.rst block_initialization.svg
	rst2html.py messages.rst >messages.html

clean:
	rm -f messages.html

.pic.svg:
	pic2plot -Tsvg $< >$@

.SUFFIXES: .pic .svg

.PHONY: all clean
