# Aerospike Document API (Python)

Verified to work on Python 3.9.13.

## Developing

To install dependencies for development:
```
pip install -r requirements.txt
```

### Documentation

To build documentation:
```
cd docs/
sphinx-build -b html . htmldir
```

### Unit testing

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

### Codestyle

This code uses [flake8](https://github.com/pycqa/flake8) for codestyle checking.

To run codestyle tests:
```
flake8 . --count --max-complexity=10 --show-source --max-line-length=127 --statistics
```
