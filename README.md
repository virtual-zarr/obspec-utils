# Obspec Utils

[![PyPI - Version](https://img.shields.io/pypi/v/obspec-utils.svg)](https://pypi.org/project/obspec-utils)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/obspec-utils.svg)](https://pypi.org/project/obspec-utils)

-----

## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```bash
python -m pip install obspec-utils
```

## Setup development environment

```console
git clone https://github.com/virtual-zarr/obspec-utils.git
cd obspec-utils
uv sync --all-groups
uv run --all-groups pytest --cov-config=pyproject.toml --cov=pkg --cov-report xml --cov=src
```

## License

`obspec-utils` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
