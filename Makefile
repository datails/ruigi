.PHONY: help clean dev docs package test deploy ci

VERSION ?= $(shell grep current_version .bumpversion.cfg | sed -E 's/.*=//g;s/ //g')
TAG ?= $(VERSION)

clean:
	@rm -rf dist/*

dev:
	@pip3 --quiet install -e .

package: clean
	@echo "~~~ Packaging ruigi"
	@python3 setup.py sdist
	@python3 setup.py bdist_wheel

bump_patch:
	@bumpversion patch

bump_minor:
	@bumpversion minor
