
ENGAGE_EXT_HOME=$(shell pwd)

all:
	mkdir -p $(ENGAGE_EXT_HOME)/datablox/sw_packages
	cd $(ENGAGE_EXT_HOME)/datablox/sw_packages; tar cvzf datablox_framework.tgz $(ENGAGE_EXT_HOME)/../datablox_framework
	


clean:
	rm -rf $(ENGAGE_EXT_HOME)/datablox/sw_packages

.PHONY: all clean
