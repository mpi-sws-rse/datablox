
all:
	cd engage; make all


clean:
	cd datablox_framework; rm -rf ./build ./dist ./*.egg-info ./datablox_framework/*.json
	cd engage; make clean

.PHONY: all clean
