# Contributing to deltabridge

## Suggesting changes
1. Create an [issue](https://github.com/datamole-ai/deltabridge/issues) describing the change you want to make.

## General workflow

### Environment setup
deltabridge uses [uv](https://docs.astral.sh/uv/) for managing dependencies.
Follow the instructions on the uv website to install it.
```bash
# Create the virtual environment and install all dependencies, including the
# dev dependency group. uv downloads Python automatically if it's missing.
uv sync

# Activate the virtual environment in the current shell
source .venv/bin/activate
```

You can also use `uv run` to run commands in the virtual environment without activating it in the current shell, e.g. `uv run pytest`.


### Test the newly implemented changes
Create unit tests by creating a Python script in the folder `tests` prefixed with `test_`.
The script should contain functions also prefixed with `test_` that make assertions.
See the `tests` folder for reference.

### Dependency audit
CI runs `uv audit --locked` to find known vulnerabilities in the locked dependencies.
Run the same command locally:
```bash
uv audit --locked
```

If the audit reports a vulnerability, read the advisory.
Update the affected package and run the audit again:
```bash
uv lock --upgrade-package PACKAGE
uv audit --locked
```
If a version constraint blocks the update, change it in `pyproject.toml`.
Run `uv lock` again.

If you cannot apply a fix, record the reason in the pull request.
Link a follow-up issue in the same pull request.
Add the advisory ID to the audit command in `.github/workflows/test.yaml`:
```bash
uv audit --locked --ignore GHSA-xxxx-xxxx-xxxx
```
Use `--ignore`, not `--ignore-vulnerability`; the pinned uv version does not support the latter.
Remove the exception when you can apply the fix.
If no fixed version exists, use `--ignore-until-fixed GHSA-xxxx-xxxx-xxxx` instead.
That exception stops working when a fixed version becomes available.

## Pull Requests & Git

* Split your work into separate and atomic pull requests. Put any
  non-obvious reasoning behind any change to the pull request description.
  Separate “preparatory” changes and modifications from new features &
  improvements.
* The pull requests are squashed when merged. The PR title is used as the commit title.
  The PR description is used as the commit description.
* Use conventional commit messages in the PR title and description.
  See [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
  Usage of conventional commit PR titles and descriptions is enforced by the CI pipeline.
* Prefer adding new commits over amending existing ones during the review process.
  The latter makes it harder to review changes and track down modifications.


## Code style

* The line length is limited to 79 characters in Python code,
except if it would make the code less readable.
* `ruff` is used for formatting and linting Python code.
The following commands can be used to properly format the code and check
for linting errors with automatic fixing:
```bash
uv run ruff format .
uv run ruff check . --fix
```
The following commands can be used to check if the code is properly
formatted and check for linting errors:
```bash
uv run ruff format --check .
uv run ruff check .
```

All of the above code style requirements are enforced by the CI pipeline.
