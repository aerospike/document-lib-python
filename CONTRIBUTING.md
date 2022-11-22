# Developers Guide

## Setup

Please use the supported Python versions of this project for development.

1. Install [Poetry](https://python-poetry.org/docs/#installation).
2. Create a virtual environment in Poetry and activate it in a new shell:

```
poetry shell
```

3. Install the project and its dependencies in editable mode:

```
poetry install
```

Editable mode allows you to make changes to the project and test those changes without recompiling the code.

## Unit testing

For now, unit tests rely on an Aerospike server to work.
Run a local Aerospike server on a Docker container:

```bash session
# Get the server image if not already downloaded
docker pull aerospike/aerospike-server

# Start server
docker run --name aerospike -d -p 3000:3000 aerospike/aerospike-server
```

Wait a few seconds for Aerospike server to fully start.
Then run the tests:

```bash session
python3 tests/test.py
```

## Documentation

To build documentation:

```bash session
cd docs/
sphinx-build -b html . htmldir -W
```

Spelling errors must be corrected using:

```bash session
sphinx-build -b spelling . build -W
```

## Codestyle

This code uses [flake8](https://github.com/pycqa/flake8) for codestyle checking.

To run codestyle tests (already configured in `.flake8` for this project):

```bash session
flake8
```

## Adding features

This project depends on a [fork](https://github.com/aerospike-community/jsonpath-ng) (i.e custom version) of the jsonpath-ng library. To add features:
1. Create a pull request in the fork with your proposed changes. Once the changes are approved, a new version of the library will be published to PyPI.
2. Create another pull request in this repository to point to the updated version of the library. The next release of this library will use the new library version.

## Cleanup

```bash session
# Stop an delete the server
docker container stop aerospike
docker container rm aerospike

# Deactivate the virtual environment and exit the shell
exit
```
