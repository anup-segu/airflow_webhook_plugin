SHELL := /bin/bash
export PACKAGE_NAME=airflow_webhook_plugin

# Airflow related settings for testing
export AIRFLOW_HOME=./tests/fixtures
export AIRFLOW__CORE__LOAD_EXAMPLES=false

setup:
	@pip install -r development.txt
	@pre-commit install

setup_db:
	@airflow initdb

test: setup
	@py.test --cov-report term-missing:skip-covered --cov=$(PACKAGE_NAME) tests/

pre_commit: setup
	@pre-commit run --all-files
