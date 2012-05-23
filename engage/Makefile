
SHELL=/bin/bash
BLOX_HOME=$(shell cd ..; pwd)
ENGAGE_EXT_HOME=$(shell pwd)
BLOXPATH=$(shell cd ../blox; pwd)
ENGAGE_DIST=$(ENGAGE_EXT_HOME)/engage-dist
TEST_DEPLOY_HOME=$(shell cd ~; pwd)/apps
PACKAGE_DIR=$(ENGAGE_EXT_HOME)/packages
ADAPTER_DIR=$(ENGAGE_EXT_HOME)/adapter_pkg
PLATFORM=$(shell ./get_platform.sh)
DJM_SRC=$(shell cd ../..; pwd)/djm

TEST_LOG_LEVEL=DEBUG

# The following two are only valid if you are running an internal build
# at genForma.
GENFORMA_CODE_HOME=$(shell cd ../../; pwd)/code
ENGAGE_BUILD_OUTPUT=$(GENFORMA_CODE_HOME)/build_output/engage

all: engage-ext engage-dist

engage-ext:
	mkdir -p $(ENGAGE_EXT_HOME)/datablox/sw_packages
	@cd $(ENGAGE_EXT_HOME)/..; tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_framework.tgz datablox_framework
	@echo "Created package datablox_framework.tgz"
	cd $(ENGAGE_EXT_HOME); tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/datablox_engage_adapter.tgz adapter_pkg
	@echo "Created package datablox_engage_adapter.tgz"
	@cd $(BLOXPATH); for dir in `ls`; do if [ -d "$$dir" ]; then tar czf $(ENGAGE_EXT_HOME)/datablox/sw_packages/$$dir.tgz $$dir; echo "Created package $$dir.tgz"; fi; done
	cp $(BLOXPATH)/solr_index__1_0/schema.xml $(ENGAGE_EXT_HOME)/datablox/sw_packages/solr_schema.xml
	cp $(ENGAGE_EXT_HOME)/setup_caretaker.sh $(ENGAGE_EXT_HOME)/datablox/sw_packages
	@echo "Add DJM if present"
	if [ -d $(DJM_SRC)/dist_job_mgr ]; then cd $(DJM_SRC); make sdist; cp dist/dist_job_mgr-0.1.0.tar.gz $(ENGAGE_EXT_HOME)/datablox/sw_packages; fi
	@echo "Built Engage extension"



# Build a version of engage with the datablox extension
engage-dist: engage-ext
	cd $(ENGAGE_EXT_HOME); ./get_engage.sh
	cd $(ENGAGE_DIST); ./install_extension.py -u ../datablox
	cp ./install_datablox.py $(ENGAGE_DIST)/install_datablox.py


# download any packages that are particularly costly to download so we
# can include in the distribution. This is mainly for testing. Need to think
# about what to do in the distributed deployment scenario.
download-packages: $(PACKAGE_DIR)
	cd $(PACKAGE_DIR); make -f ../makefile.packages.$(PLATFORM) all

$(PACKAGE_DIR):
	mkdir -p $(PACKAGE_DIR)

# just download packages needed for testing
test-packages: $(PACKAGE_DIR)
	cd $(PACKAGE_DIR); make -f ../makefile.packages.$(PLATFORM) test 

test-deploy: #test-packages
	@echo "Running test deploy"
	if [ -f $(TEST_DEPLOY_HOME)/config/installed_resources.json ]; then $(TEST_DEPLOY_HOME)/engage/bin/svcctl stop; fi
	rm -rf $(TEST_DEPLOY_HOME)
	if [ -d $(PACKAGE_DIR) ]; then cp $(PACKAGE_DIR)/* $(ENGAGE_DIST)/sw_packages; fi
	cd $(ENGAGE_DIST); ./install_datablox.py $(TEST_DEPLOY_HOME)

# Run a configuration involving dir-src, file-mongo, and mongo-map-reduce.
# At the end, look for file type records (should be 2).
test-map-reduce:
	# substitute BLOX_HOME for the ~/ in the file map reduce scenario, so that
	# we only crawl the datablox source tree
	python -c "import sys; sys.stdout.write(open(sys.argv[1], 'rb').read().replace('~/apps', sys.argv[2]))" $(BLOX_HOME)/examples/file_analytics.json $(BLOX_HOME) >$(TEST_DEPLOY_HOME)/file_map_reduce.json
	@echo "Running map-reduce scenario"
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl start
	cd $(TEST_DEPLOY_HOME)/python/bin; ./datablox-master --log-level=$(TEST_LOG_LEVEL) $(TEST_DEPLOY_HOME)/file_map_reduce.json master
	$(TEST_DEPLOY_HOME)/mongodb-2.0/bin/mongo --eval "db.getSisterDB('file_db').file_types.count()"
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl stop
	@echo "Map-reduce completed successfully"

test-read-files:
	@echo "Running read-files scenario"
	python -c "import sys; sys.stdout.write(open(sys.argv[1], 'rb').read().replace('.', sys.argv[2]))" $(BLOX_HOME)/examples/read_files.json $(BLOX_HOME)/blox >$(TEST_DEPLOY_HOME)/read_files.json
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl start
	cd $(TEST_DEPLOY_HOME)/python/bin; ./datablox-master --log-level=$(TEST_LOG_LEVEL) $(TEST_DEPLOY_HOME)/read_files.json master
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl stop
	@echo "Read-files completed successfully"

test-simple-shard:
	@echo "Running simple-shard scenario"
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl start
	cd $(TEST_DEPLOY_HOME)/python/bin; ./datablox-master --log-level=$(TEST_LOG_LEVEL) $(BLOX_HOME)/examples/simple_shard.json master
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl stop
	@echo "simple-shard completed successfully"

# since this test take a long time to run, it is not run as a part of "make test"
test-big-crawl:
	@echo "Running test-big-crawl scenario"
	python $(BLOX_HOME)/scripts/generate_file_data.py ~/apps/test_data 150000
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl start
	cd $(TEST_DEPLOY_HOME)/python/bin; ./datablox-master --log-level=$(TEST_LOG_LEVEL) $(BLOX_HOME)/examples/join_bug1.json master
	$(TEST_DEPLOY_HOME)/engage/bin/svcctl stop
	@echo "test-big-crawl completed successfully"


test: engage-ext engage-dist test-deploy test-map-reduce test-deploy test-simple-shard
	@echo "All tests completed successfully"

kill-test-procs:
	$(BLOX_HOME)/scripts/killproc -n python
	$(BLOX_HOME)/scripts/killproc -n mongo
	$(BLOX_HOME)/scripts/killproc -n solr/example/start.jar


help:
	@echo "Targets are all engage-ext engage-dist test-deploy test-map-reduce test-read-files test test-big-crawl download-packages kill-test-procs clean clean-all"

clean:
	rm -rf $(ENGAGE_EXT_HOME)/datablox/sw_packages
	rm -rf $(ENGAGE_DIST)
	rm -rf $(ADAPTER_DIR)/dist
	rm -rf $(ADAPTER_DIR)/build

# clean everything, including the package dir
clean-all: clean
	rm -rf $(PACKAGE_DIR)

.PHONY: all clean clean-all engage-dist engage-ext help download-packages test-deploy test-map-reduce test-read-files test-simple-shard test test-packages kill-test-procs test-big-crawl
