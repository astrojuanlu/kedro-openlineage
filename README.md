# kedro-openlineage

[![Documentation Status](https://readthedocs.org/projects/kedro-openlineage/badge/?version=latest)](https://kedro-openlineage.readthedocs.io/en/latest/?badge=latest)
[![Code style: ruff-format](https://img.shields.io/badge/code%20style-ruff_format-6340ac.svg)](https://github.com/astral-sh/ruff)
[![PyPI](https://img.shields.io/pypi/v/kedro-openlineage)](https://pypi.org/project/kedro-openlineage)

OpenLineage integration for Kedro

## Installation

To install, run

```
$ uv pip install kedro-openlineage
```

## Overview

After installing the plugin, configure the OpenLineage integration.
For example, using Marquez as explained in https://openlineage.io/docs/client/python/#start-docker-and-marquez

```
$ cat conf/base/openlineage.yml
transport:
  type: http
  url: http://localhost:5000
```

And that's it! Now do `kedro run` and you should be able to see the events.

## Development

To run style checks:

```
$ uv tool install pre-commit
$ pre-commit run -a
```