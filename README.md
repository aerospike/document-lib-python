# Aerospike Document API (Python)

Python port of the Aerospike Document API in [Java](https://github.com/aerospike/aerospike-document-lib).

This is currently in beta, so there may be bugs or features that aren't supported yet.

## More Info
- [API Documentation](https://document-library-python.readthedocs.io/en/latest/)
- [PyPI Package](https://pypi.org/project/document-lib-python/)

## Compatibility
- All supported Python versions from the Python client
- Aerospike Python Client 6.x - 8.x

## Features

Not all JSONPath queries are currently supported. Here is a list of currently supported features:

| Supported? | Feature              | Examples                                            |
|------------|----------------------|-----------------------------------------------------|
| [x]        | Map access           | `$.item, $["item"]`                                 |
| [x]        | List access          | `$.list[0]`                                         |
| [x]        | Wildcard map access  | `$.*`                                               |
| [x]        | Wildcard list access | `$.list[*]`                                         |
| [x]        | List slices          | `$.list[2:4]`                                       |
| [x]        | List step slices     | `$.list[2:4:1]`                                     |
| [x]        | List set of indices  | `$.list[2,4]`                                       |
| [x]        | Recursive access     | `$..item`                                           |
| [x]        | Exists filter        | `$.listOfMaps[?(@.mapitem)]`                        |
| [x]        | Comparison filter    | `$.listOfMaps[?(@.mapitem > 10)]`                   |
| [x]        | And filter           | `$.listOfMaps[?(@.mapitem > 10 & @.mapitem < 50)]`  |
| [x]        | Or filter            | `$.listOfMaps[?(@.mapitem < 10 \| @.mapitem > 50)]` |
| [ ]        | Variable filter      | `$.listOfMaps[?(@.mapitem < $['rootitem'])]`        |
| [x]        | Regex filter*        | `$.listOfMaps[?(@.mapitem =~ "(?i).*mesa")]`        |
| [-]        | Functions            | `$.list.length()` supported                         |

\* Python regex expressions are used

## Contributing

To add features to this project, please see `CONTRIBUTING.md` in the repository source code.
