
all:
	cd engage; make all
	@echo "Completed datablox build"

help:
	@echo "targets are: all test test-deploy kill-test-procs clean clean-all"

test:
	cd engage; make test

test-deploy:
	cd engage; make test-deploy

kill-test-procs:
	cd engage; make kill-test-procs

clean:
	cd datablox_framework; rm -rf ./build ./dist ./*.egg-info ./datablox_framework/*.json
	cd engage; make clean

# clean everything, including downloaded packages
clean-all: clean
	cd engage; make clean-all

.PHONY: all clean clean-all test-deploy kill-test-procs help
