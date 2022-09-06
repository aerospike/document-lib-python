# Aerospike Document API (Python)

Python port of the Aerospike Document API in [Java](https://github.com/aerospike/aerospike-document-lib).

This is currently in beta, so there may be bugs or features that aren't supported yet.

## Compatibility
- Python 3.7.x - 3.9.13.
- Aerospike Python Client 7.0.2

## Features

Not all JSONPath queries are currently supported. Here is a list of currently supported features:

| Supported? | Feature              | Examples                                            |
|------------|----------------------|-----------------------------------------------------|
| [x]        | Map access           | `$.item, $["item"]`                                 |
| [x]        | List access          | `$.list[0]`                                         |
| [x]        | Wildcard map access  | `$.*`                                               |
| [x]        | Wildcard list access | `$.list[*]`                                         |
| [x]        | List slices          | `$.list[2:4]`                                       |
| [ ]        | List step slices     | `$.list[2:4:1]`                                     |
| [ ]        | List set of indices  | `$.list[2,4]`                                       |
| [x]        | Recursive access     | `$..item`                                           |
| [x]        | Exists filter        | `$.listOfMaps[?(@.mapitem)]`                        |
| [x]        | Comparison filter    | `$.listOfMaps[?(@.mapitem > 10)]`                   |
| [x]        | And filter           | `$.listOfMaps[?(@.mapitem > 10 & @.mapitem < 50)]`  |
| [ ]        | Or filter            | `$.listOfMaps[?(@.mapitem < 10 \| @.mapitem > 50)]` |
| [ ]        | Variable filter      | `$.listOfMaps[?(@.mapitem < $['rootitem'])]`        |
| [ ]        | Regex filter         | `$.listOfMaps[?(@.mapitem ~= /.*mesa/i)]`           |
| [-]        | Functions            | `$.list.length()` supported                         |

## Contributing

To add features to this project, please see `CONTRIBUTING.md` in the repository source code.
