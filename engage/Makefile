
SHELL=/bin/bash
ENGAGE_EXT_HOME=$(shell pwd)
BLOXPATH=$(shell cd ../blox; pwd)

all:
	mkdir -p $(ENGAGE_EXT_HOME)/datablox/sw_packages
	@cd $(ENGAGE_EXT_HOME)/..; tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_framework.tgz datablox_framework
	@echo "Created package datablox_framework.tgz"
	@cd $(BLOXPATH); for dir in `ls`; do if [ -d "$$dir" ]; then tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/$$dir.tgz $$dir; echo "Created package $$dir.tgz"; fi; done
	@echo "Built Engage extension"


clean:
	rm -rf $(ENGAGE_EXT_HOME)/datablox/sw_packages

.PHONY: all clean
