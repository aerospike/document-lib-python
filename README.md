# Aerospike Document API (Python)

Verified to work on Python 3.9.13.

## Developing

To install dependencies:
```
pip install -r requirements.txt
```

To build documentation:
```
cd docs/
sphinx-build -b html . htmldir
```

## Tests

```
cd tests/
python3 test.py
```

## Codestyle

This code uses [flake8](https://github.com/pycqa/flake8) for codestyle checking.

To run codestyle tests:
```
flake8 . --count --max-complexity=10 --show-source --max-line-length=127 --statistics
```
