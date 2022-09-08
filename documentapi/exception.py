class JsonPathMissingRootError(ValueError):
    """This is thrown when the JSON path doesn't start with a ``$``."""

    def __init__(self, jsonPath):
        message = f"JSON path must start with $: {jsonPath}"
        super().__init__(message)


class JsonPathParseError(ValueError):
    """This is thrown when the JSON path has invalid syntax."""

    def __init__(self, jsonPath):
        message = f"Unable to parse JSON path: {jsonPath}"
        super().__init__(message)


class JSONNotFoundError(ValueError):
    """This is thrown when an object can't be found with the JSON path."""

    def __init__(self, jsonPath):
        message = f"Unable to access document object with JSON path {jsonPath}"
        super().__init__(message)
