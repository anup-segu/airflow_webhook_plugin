SHELL := /bin/bash
export PACKAGE_NAME=airflow_webhook_plugin

setup:
	@pip install -r development.txt
	@pre-commit install

test: setup
	@py.test --cov-report term-missing:skip-covered --cov=$(PACKAGE_NAME) tests/

pre_commit: setup
	@pre-commit run --all-files
