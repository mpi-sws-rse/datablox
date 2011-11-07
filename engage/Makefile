
ENGAGE_EXT_HOME=$(shell pwd)

all:
	mkdir -p $(ENGAGE_EXT_HOME)/datablox/sw_packages
	cd $(ENGAGE_EXT_HOME)/..; tar cvzf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_framework.tgz datablox_framework


clean:
	rm -rf $(ENGAGE_EXT_HOME)/datablox/sw_packages

.PHONY: all clean
