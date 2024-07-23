# Collector for CESA Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-cesa/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-cesa/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-cesa/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-cesa?branch=main)

This script connects daily to the
[Climate Emergency Software Alliance](https://cesa.global/)
(CESA) [API](https://docs.petabencana.id/v/master-1)
to obtain the
[crowdsourced disaster reports](https://docs.petabencana.id/v/master-1/routes/crowdsourced-reports)
for the past week and upload them to HDX.

### Usage

    python run.py

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key **hdx-scraper-who** as specified
 in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

## Development

### Pre-commit

Be sure to install `pre-commit`, which is run every time
you make a git commit:

```shell
pip install pre-commit
pre-commit install
```

The configuration file for this project is in a
non-start location. Thus, you will need to edit your
`.git/hooks/pre-commit` file to reflect this. Change
the first line that begins with `ARGS` to:

    ARGS=(hook-impl --config=.config/pre-commit-config.yaml --hook-type=pre-commit)

With pre-commit, all code is formatted according to
[black]("https://github.com/psf/black") and
[ruff]("https://github.com/charliermarsh/ruff") guidelines.

To check if your changes pass pre-commit without committing, run:

    pre-commit run --all-files --config=.config/pre-commit-config.yaml

## Testing

Ensure you have the required packages to run the tests:

    pip install -r requirements-test.txt

To run the tests and view coverage, execute:

    pytest -c .config/pytest.ini --cov=. --cov-config .config/coveragerc

## Packages

[pip-tools](https://github.com/jazzband/pip-tools) is used for
package management.  If you’ve introduced a new package to the
source code please add it to the `dependencies` section of
`pyproject.toml` with any known version constraints.

For adding packages for testing, add them to
the `test` sections under `[project.optional-dependencies]`.

Any changes to the dependencies will be automatically reflected in
`requirements.txt` and `requirements-test.txt` with `pre-commit`,
but you can re-generate the file without committing by executing:

    pre-commit run pip-compile --all-files --config=.config/pre-commit-config.yaml
