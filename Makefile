.PHONY: fmt help

help:
	@echo "Available targets:"
	@echo "  fmt   Format the code using black"

fmt:
	ruff check --select I --fix
	ruff format

lint:
	ruff check
