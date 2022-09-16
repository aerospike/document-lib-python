# Developers Guide

## Setup

Create a new virtual environment to test the code (recommended):
```
python3 -m venv .venv
. .venv/bin/activate
```

To install dependencies for development:
```
pip install -r requirements.txt
```

## Unit testing

The tests use a locally built and installed module for this code:
```
# Build current code and install package
pip install --editable .
```

For now, unit tests rely on an Aerospike server to work.
Run a local Aerospike server on a Docker container:
```
# Get the server image if not already downloaded
docker pull aerospike/aerospike-server

# Start server
docker run --name aerospike -d -p 3000:3000 aerospike/aerospike-server
```

Wait a few seconds for Aerospike server to fully start.
Then run the tests:
```
python3 tests/test.py
```

## Documentation

To build documentation:
```
cd docs/
sphinx-build -b html . htmldir -W
```

## Codestyle

This code uses [flake8](https://github.com/pycqa/flake8) for codestyle checking.

To run codestyle tests:
```
cd documentapi/
flake8 . --count --max-complexity=10 --show-source --max-line-length=127
```

## Adding features

This project depends on a [fork](https://github.com/aerospike-community/jsonpath-ng) (i.e custom version) of the jsonpath-ng library. To add features:
1. Create a pull request in the fork with your proposed changes. Once the changes are approved, a new version of the library will be published to PyPI.
2. Create another pull request in this repository to point to the updated version of the library. The next release of this library will use the new library version.

## Cleanup

```
# Stop an delete the server
docker container stop aerospike
docker container rm aerospike

# Quit and delete virtual environment
deactivate
rm -r .venv
```
