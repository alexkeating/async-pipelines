local:
	$(MAKE) test

test:
	pytest --cov=.

typecheck:
	mypy -m places

init_env:
	pipenv shell
