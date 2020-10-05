.DEFAULT_GOAL := default

default:
	@echo Do not use this makefile to build hail, for information on how to \
	     build hail see: https://hail.is/docs/0.2/
	@false

.PHONY: check-all
check-all: check-hail check-services check-benchmark-service

.PHONY: check-hail
check-hail:
	$(MAKE) -C hail/python check

.PHONY: check-services
check-services: check-auth check-batch check-ci check-gear check-memory \
  check-notebook check-query check-router-resolver check-scorecard check-web-common

.PHONY: check-auth
check-auth:
	$(MAKE) -C auth check

.PHONY: check-batch
check-batch:
	$(MAKE) -C batch check

.PHONY: check-benchmark-service
check-benchmark-service:
	$(MAKE) -C benchmark-service check

.PHONY: check-ci
check-ci:
	$(MAKE) -C ci check

.PHONY: check-gear
check-gear:
	$(MAKE) -C gear check

.PHONY: check-memory
check-memory:
	$(MAKE) -C memory check

.PHONY: check-notebook
check-notebook:
	$(MAKE) -C notebook check

.PHONY: check-query
check-query:
	$(MAKE) -C query check

.PHONY: check-router-resolver
check-router-resolver:
	$(MAKE) -C router-resolver check

.PHONY: check-scorecard
check-scorecard:
	$(MAKE) -C scorecard check

.PHONY: check-web-common
check-web-common:
	$(MAKE) -C web_common check
