local:
	$(MAKE) test

test:
	pytest --cov=.

typecheck:
	mypy -m places

lint:
	black .
	flake8 . --count
ci:
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test

init_env:
	pipenv shell
