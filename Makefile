.DEFAULT_GOAL := build
.PHONY: build publish publish-flightcheck docs venv


build: coverage publish-flightcheck
	@echo No python build is defined.

freeze:
	pip freeze > requirements.txt

coverage: test
	mkdir -p docs/build/html
	coverage html

docs: coverage
	mkdir -p docs/source/_static
	mkdir -p docs/source/_templates
	cd docs && $(MAKE) html

publish-flightcheck: freeze
	python setup.py sdist

publish:
	python setup.py sdist upload -r pypi

clean :
	rm -rf dist \
	rm -rf docs/build \
	rm -rf *.egg-info
	coverage erase

test:
	py.test --cov . tests/

venv :
	virtualenv --python python3.6 venv

install:
	pip install -r requirements.txt

