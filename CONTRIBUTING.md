# Developers Guide

To install dependencies for development:
```
pip install -r requirements-dev.txt
```

## Documentation

To build documentation:
```
cd docs/
sphinx-build -b html . htmldir
```

## Unit testing

The tests use a locally built and installed module for this code:
```
# Create a new virtual environment to test the code
python3 -m venv .venv
. .venv/bin/activate
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
cd tests/
python3 test.py
```

Once you are done testing:
```
# Stop an delete the server
docker container stop aerospike
docker container rm aerospike
# Quit and delete virtual environment
deactivate
rm -r .venv
```

## Codestyle

This code uses [flake8](https://github.com/pycqa/flake8) for codestyle checking.

To run codestyle tests:
```
cd documentapi/
flake8 . --count --max-complexity=10 --show-source --max-line-length=127 --statistics
```

## Adding features

This project depends on a [fork](https://github.com/aerospike-community/jsonpath-ng) (i.e custom version) of the jsonpath-ng library. To add features, you may have to create a pull request in the fork as well as in this repository. Then, the project will use the latest commit of the forked library in the following release.

## Custom JSONPath library

The project's code directly imports the library's modules. Because of this, the project needs to install the library's dependencies on top of it's own.
