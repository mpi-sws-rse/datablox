
all:
	cd engage; make all

test:
	cd engage; make test


clean:
	cd datablox_framework; rm -rf ./build ./dist ./*.egg-info ./datablox_framework/*.json
	cd engage; make clean

# clean everything, including downloaded packages
clean-all: clean
	cd engage; make clean-all

.PHONY: all clean clean-all
