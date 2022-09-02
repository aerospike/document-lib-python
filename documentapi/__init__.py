import aerospike
from aerospike import Client
from aerospike import exception as ex

from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations

from jsonpath_ng.ext import parse

import re

from typing import Any, List, Tuple, Union

from .exception import JsonPathMissingRootError, JsonPathParseError, ObjectNotFoundError

class DocumentClient:
    """Client to run JSON queries"""

    def __init__(self, client: Client):
        self.client = client

    def get(self, key: tuple, binName: str, jsonPath: str, readPolicy: dict = None) -> Any:
        """
        Get object(s) from a JSON document using JSON path.

        If multiple objects are matched, they will be returned as a :class:`list`.
        A list of results does not have a guaranteed order.

        Otherwise, the object itself is returned.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path to retrieve the object
        :param dict readPolicy: the read policy for get() operation

        :return: :py:obj:`Any`
        :raises: :exc:`JsonPathMissingRootError` when the JSON path doesn't start with a ``$``
        :raises: :exc:`JsonPathParseError` when the JSON path has a syntax error
        :raises: :exc:`ObjectNotFoundError` when there are no matches with the JSON path
        """

        self.validateJsonPath(jsonPath)

        jsonPath, advancedJsonPath = self.divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = self.tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = self.buildContextArray(tokens)

        operatePolicy = self.convertToOperatePolicy(readPolicy)

        getOp = self.createGetOperation(binName, ctxs, lastToken)
        fetchedDocument = self.getSmallestDocument(key, binName, getOp, operatePolicy, jsonPath)

        # Use JSONPath library to perform advanced ops on fetched document
        if advancedJsonPath:
            jsonPathExpr = parse(advancedJsonPath)
            fetchedDocument = [match.value for match in jsonPathExpr.find(fetchedDocument)]

        return fetchedDocument

    def put(self, key: tuple, binName: str, jsonPath: str, obj: Any, writePolicy: dict = None):
        """
        Put an object into a JSON document using JSON path.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path location to store the object
        :param dict writePolicy: the write policy for put() operation

        :raises: :exc:`JsonPathMissingRootError` when the JSON path doesn't start with a ``$``
        :raises: :exc:`JsonPathParseError` when the JSON path has a syntax error
        :raises: :exc:`ObjectNotFoundError` when there are no matches with the JSON path
        """
        self.validateJsonPath(jsonPath)

        jsonPath, advancedJsonPath = self.divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = self.tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = self.buildContextArray(tokens)

        operatePolicy = self.convertToOperatePolicy(writePolicy)

        if advancedJsonPath:
            # If an advanced operation exists in JSON path
            # We need to fetch the whole document before it
            # and modify the document
            getOp = self.createGetOperation(binName, ctxs, lastToken)
            smallestDocument = self.getSmallestDocument(key, binName, getOp, operatePolicy, jsonPath)

            # Update smallest document
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.update(smallestDocument, obj)
        else:
            smallestDocument = obj

        # Send updated document to server
        putOp = self.createPutOperation(binName, ctxs, lastToken, smallestDocument)
        self.sendSmallestDocument(key, putOp, operatePolicy, jsonPath)

    def append(self, key: tuple, binName: str, jsonPath: str, obj, writePolicy: dict = None):
        """
        Append an object to a list in a JSON document using JSON path.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path ending with a list
        :param dict writePolicy: the write policy for operate() operation

        :raises: :exc:`JsonPathMissingRootError` when the JSON path doesn't start with a ``$``
        :raises: :exc:`JsonPathParseError` when the JSON path has a syntax error
        :raises: :exc:`ObjectNotFoundError` when there are no matches with the JSON path
        """
        self.validateJsonPath(jsonPath)

        jsonPath, advancedJsonPath = self.divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = self.tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = self.buildContextArray(tokens)

        op = self.createGetOperation(binName, ctxs, lastToken)

        # Get document from server
        operatePolicy = self.convertToOperatePolicy(writePolicy)
        smallestDocument = self.getSmallestDocument(key, binName, op, operatePolicy, jsonPath)

        if advancedJsonPath:
            # Append object to all matching lists
            jsonPathExpr = parse(advancedJsonPath)
            matches = jsonPathExpr.find(smallestDocument)
            for match in matches:
                match.value.append(obj)
                match.full_path.update(match.value)
        else:
            # List is the whole document
            smallestDocument.append(obj)

        # Send new document to server
        op = self.createPutOperation(binName, ctxs, lastToken, smallestDocument)
        self.sendSmallestDocument(key, op, operatePolicy, jsonPath)

    def delete(self, key: tuple, binName: str, jsonPath: str, writePolicy: dict = None):
        """
        Delete an object in a JSON document using JSON path.

        Deleting the root element causes the bin to contain an empty dictionary.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path of object to delete
        :param dict writePolicy: the write policy for operate() operation

        :raises: :exc:`JsonPathMissingRootError` when the JSON path doesn't start with a ``$``
        :raises: :exc:`JsonPathParseError` when the JSON path has a syntax error
        :raises: :exc:`ObjectNotFoundError` when there are no matches with the JSON path
        """
        self.validateJsonPath(jsonPath)

        jsonPath, advancedJsonPath = self.divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = self.tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = self.buildContextArray(tokens)

        operatePolicy = self.convertToOperatePolicy(writePolicy)

        if advancedJsonPath:
            # Fetch document from server
            op = self.createGetOperation(binName, ctxs, lastToken)
            smallestDocument = self.getSmallestDocument(key, binName, op, operatePolicy, jsonPath)

            # Delete all matches
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.filter(lambda _: True, smallestDocument)

            # Send new document to server
            op = self.createPutOperation(binName, ctxs, lastToken, smallestDocument)
            self.sendSmallestDocument(key, op, operatePolicy, jsonPath)
            return

        # Delete entire matched item
        # Create delete operation
        if type(lastToken) == int:
            op = list_operations.list_pop(binName, lastToken, ctx=ctxs)
        elif lastToken == "$":
            # Replace bin with empty dict
            op = operations.write(binName, {})
        else:
            op = map_operations.map_remove_by_key(binName, lastToken, aerospike.MAP_RETURN_NONE, ctx=ctxs)

        # Tada
        try:
            # Deleting a missing key does not throw an error
            # User must check that it exists beforehand
            self.client.operate(key, [op], operatePolicy)
        except (ex.OpNotApplicable, ex.InvalidRequest):
            # OpNotApplicable: deleting from missing list/map, out of bounds index
            # InvalidRequest: deleting index from map or key from list
            raise ObjectNotFoundError(jsonPath)

    # Helper functions

    # These functions handle possible errors from calling operate()
    # Pass in JSON path in case we throw an error

    def getSmallestDocument(self, key, binName, op, operatePolicy, jsonPath):
        try:
            _, _, bins = self.client.operate(key, [op], operatePolicy)
            fetchedDocument = bins[binName]
            if fetchedDocument is None:
                # Caused by using a key that doesn't exist in a map
                raise ObjectNotFoundError(jsonPath)
        except (ex.BinIncompatibleType, ex.InvalidRequest, ex.OpNotApplicable):
            # InvalidRequest: index get() on a map or primitive
            # BinIncompatibleType: key get() on a list or primitive
            # OpNotApplicable: get() from missing list/map or out of bounds index
            raise ObjectNotFoundError(jsonPath)
        return fetchedDocument

    def sendSmallestDocument(self, key, op, operatePolicy, jsonPath):
        try:
            _, _, _ = self.client.operate(key, [op], operatePolicy)
        except ex.OpNotApplicable:
            # OpNotApplicable:
            # - put() into map as list
            # - put() into list as map
            # - put() into missing list/map
            raise ObjectNotFoundError(jsonPath)

    @staticmethod
    def validateJsonPath(jsonPath: str):
        # JSON path must start at document root
        if not jsonPath or jsonPath.startswith("$") is False:
            raise JsonPathMissingRootError(jsonPath)

        # Check for syntax errors
        try:
            parse(jsonPath)
        except Exception:
            raise JsonPathParseError(jsonPath)

    # Divide JSON path into two parts
    # The first part does not have advanced operations
    # The second part starts with the first advanced operation in the path
    @staticmethod
    def divideJsonPath(jsonPath: str) -> Tuple[str, Union[str, None]]:
        # Get substring in path beginning with the first advanced operation
        # Look for operations in path
        ADVANCED_OP_TOKENS = ["[*]", "..", "[?", ".length()"]
        startIndices = [jsonPath.find(op) for op in ADVANCED_OP_TOKENS]
        # Filter out ones that aren't found
        startIndices = list(filter(lambda index: index >= 1, startIndices))
        if startIndices:
            startIndex = min(startIndices)
        else:
            # No advanced operations found
            startIndex = -1

        advancedJsonPath = None
        if startIndex > 0:
            # Treat fetched JSON document as root document
            advancedJsonPath = "$" + jsonPath[startIndex:]
            jsonPath = jsonPath[:startIndex]

        return jsonPath, advancedJsonPath

    # Split up JSON path without advanced operations
    # into map and list access tokens
    @staticmethod
    def tokenize(firstJsonPath: str) -> List[str]:
        # First divide JSON path into "big" tokens
        # using "." separator
        # Example:
        # "$[1].b.c['test']" -> ["$[1]", "b", "c['test']]"
        bigTokens = firstJsonPath.split(".")

        # Edge case:
        # Treat * as fetching the whole data, not just its members
        if bigTokens[-1] == "*":
            bigTokens.pop()

        # Then divide each big token into "small" tokens
        # using  "[]" separator
        # Example:
        # [$[1], b, c['test']] -> ["$", "1", "b", "c", "'test'"]
        results = []
        for bigToken in bigTokens:
            smallTokens = re.split(r"\[|\]", bigToken)
            # Remove empty small tokens
            while "" in smallTokens:
                smallTokens.remove("")

            # Parse small tokens
            # Keys are encoded as strings
            # Indices are encoded as integers
            parsedFirstToken = False
            for smallToken in smallTokens:
                if not parsedFirstToken:
                    # By default, encode first token as string
                    # Since it is always either $ or a key
                    parsedFirstToken = True
                elif smallToken.startswith("'") and smallToken.endswith("'"):
                    # Keys can also be enclosed in brackets
                    # if they are surrounded by single quotes
                    # Remove quotes from key
                    smallToken = smallToken[1:-1]
                else:
                    # Otherwise assume list access inside square brackets
                    smallToken = int(smallToken)
                results.append(smallToken)
        return results

    @staticmethod
    def buildContextArray(tokens: list) -> Union[List[str], None]:
        ctxs = []
        for token in tokens:
            if type(token) == int:
                # List access
                ctx = cdt_ctx.cdt_ctx_list_index(token)
            elif token == '$':
                # Don't need context for root
                continue
            else:
                # Map access
                ctx = cdt_ctx.cdt_ctx_map_key(token)
            ctxs.append(ctx)

        if not ctxs:
            # Contexts must be populated or None
            return None

        return ctxs

    @staticmethod
    def createGetOperation(binName: str, ctxs: list, lastToken: str) -> dict:
        # Create get operation using last token
        if type(lastToken) == int:
            op = list_operations.list_get_by_index(binName, lastToken, aerospike.LIST_RETURN_VALUE, ctxs)
        elif lastToken == "$":
            # Get whole document
            op = operations.read(binName)
        else:
            op = map_operations.map_get_by_key(binName, lastToken, aerospike.MAP_RETURN_VALUE, ctxs)

        return op

    @staticmethod
    def createPutOperation(binName: str, ctxs: list, lastToken: str, obj: object) -> dict:
        # Create put operation
        if type(lastToken) == int:
            op = list_operations.list_set(binName, lastToken, obj, ctx=ctxs)
        elif lastToken == "$":
            # Get whole document
            op = operations.write(binName, obj)
        else:
            op = map_operations.map_put(binName, lastToken, obj, ctx=ctxs)

        return op

    @staticmethod
    def convertToOperatePolicy(policy: dict) -> Union[dict, None]:
        if policy is None:
            return None

        # Filter out non-operate policies
        operatePolicy = policy.copy()
        OPERATE_CONFIG_KEYS = [
            "max_retries", "sleep_between_retries", "socket_timeout", "total_timeout", "compress", "key", "gen", "replica",
            "commit_level", "read_mode_ap", "read_mode_sc", "exists", "durable_delete", "expressions"
        ]
        for key in operatePolicy:
            if key not in OPERATE_CONFIG_KEYS:
                operatePolicy.pop(key)

        return operatePolicy
