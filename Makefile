
all:
	cd engage; make all


clean:
	cd datablox_framework; rm -rf ./build ./dist ./*.egg-info
	cd engage; make clean

.PHONY: all clean
