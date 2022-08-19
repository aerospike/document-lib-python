class JsonPathMissingRootError(ValueError):
    def __init__(self, jsonPath):
        message = f"JSON path must start with $: {jsonPath}"
        super().__init__(message)

class JsonPathParseError(ValueError):
    def __init__(self, jsonPath):
        message = f"Unable to parse JSON path: {jsonPath}"
        super().__init__(message)

class ObjectNotFoundError(ValueError):
    def __init__(self, jsonPath):
        message = f"Unable to access document object with JSON path {jsonPath}"
        super().__init__(message)
