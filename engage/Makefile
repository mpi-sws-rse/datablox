
SHELL=/bin/bash
ENGAGE_EXT_HOME=$(shell pwd)
BLOXPATH=$(shell cd ../blox; pwd)
ENGAGE_DIST=$(ENGAGE_EXT_HOME)/engage-dist
TEST_DEPLOY_HOME=$(shell cd ~; pwd)/apps
PACKAGE_DIR=$(ENGAGE_EXT_HOME)/packages
ADAPTER_DIR=$(ENGAGE_EXT_HOME)/adapter_pkg

# The following two are only valid if you are running an internal build
# at genForma.
GENFORMA_CODE_HOME=$(shell cd ../../; pwd)/code
ENGAGE_BUILD_OUTPUT=$(GENFORMA_CODE_HOME)/build_output/engage

all: engage-ext

engage-ext:
	mkdir -p $(ENGAGE_EXT_HOME)/datablox/sw_packages
	@cd $(ENGAGE_EXT_HOME)/..; tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_framework.tgz datablox_framework
	@echo "Created package datablox_framework.tgz"
	cd $(ENGAGE_EXT_HOME); tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_engage_adapter.tgz adapter_pkg
	@echo "Created package datablox_engage_adapter.tgz"
	@cd $(BLOXPATH); for dir in `ls`; do if [ -d "$$dir" ]; then tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/$$dir.tgz $$dir; echo "Created package $$dir.tgz"; fi; done
	cp $(BLOXPATH)/solr_index__1_0/schema.xml $(ENGAGE_EXT_HOME)/datablox/sw_packages/solr_schema.xml
	@echo "Built Engage extension"


# The dist-from-src target assumes that there's a built engage distribution
# under ../../code/build_output/engage. The code is copied to ./engage-dist
# and then the datablox extension is installed.
# This is only useful if you are running an internal build at genForma.
dist-from-src: engage-ext
	if ! [ -d $(ENGAGE_BUILD_OUTPUT) ]; then echo "Missing engage build output at $(ENGAGE_BUILD_OUTPUT)"; exit 1; fi
	rm -rf $(ENGAGE_DIST)
	cp -r $(ENGAGE_BUILD_OUTPUT) $(ENGAGE_DIST)
	cd $(ENGAGE_DIST); ./install_extension.py ../datablox

# download any packages that are particularly costly to download so we
# can include in the distribution. This is mainly for testing. Need to think
# about what to do in the distributed deployment scenario.
download-packages: $(PACKAGE_DIR)
	cd $(PACKAGE_DIR); make -f ../makefile.packages all

$(PACKAGE_DIR):
	mkdir -p $(PACKAGE_DIR)



test-deploy:
	if [ -f $(TEST_DEPLOY_HOME)/config/installed_resources.json ]; then $(TEST_DEPLOY_HOME)/engage/bin/svcctl stop; fi
	rm -rf $(TEST_DEPLOY_HOME)
	if [ -d $(PACKAGE_DIR) ]; then cp $(PACKAGE_DIR)/* $(ENGAGE_DIST)/sw_packages; fi
	cd $(ENGAGE_DIST); ./bootstrap.py $(TEST_DEPLOY_HOME)
	cd $(TEST_DEPLOY_HOME)/engage/bin; ./deployer $(ENGAGE_EXT_HOME)/datablox/metadata/install_spec.json

help:
	@echo "Targets are all engage-ext dist-from-src test-deploy download-packages clean clean-all"

clean:
	rm -rf $(ENGAGE_EXT_HOME)/datablox/sw_packages
	rm -rf $(ENGAGE_DIST)
	rm -rf $(ADAPTER_DIR)/dist
	rm -rf $(ADAPTER_DIR)/build

# clean everything, including the package dir
clean-all: clean
	rm -rf $(PACKAGE_DIR)

.PHONY: all clean clean-all dist-from-src engage-ext help download-packages
