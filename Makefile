local:
	$(MAKE) test

test:
	pytest --cov=.

typecheck:
	mypy -m pipelines

lint:
	black .
	flake8 . --count
ci:
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test

init_env:
	pipenv shell

install_self:
	pipenv install --dev -e .
