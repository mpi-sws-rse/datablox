# compile the message sequence charts
SVGFILES=block_initialization.svg poll_all_nodes.svg liveness_check.svg stop_all.svg

THEME=--stylesheet=docutils_basic.css --embed-stylesheet

all: messages.html

messages.html: messages.rst $(SVGFILES)
	rst2html.py $(THEME) messages.rst >messages.html

clean:
	rm -f messages.html $(SVGFILES)

.pic.svg:
	pic2plot -Tsvg $< >$@

.SUFFIXES: .pic .svg

.PHONY: all clean
