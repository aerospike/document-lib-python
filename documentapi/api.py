'''
Copyright 2022 Aerospike, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from aerospike import MAP_RETURN_NONE, MAP_RETURN_VALUE, LIST_RETURN_VALUE
from aerospike import Client
from aerospike import exception as ex
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations
from aerospike_helpers import cdt_ctx

import re
from typing import Any, List, Dict, Tuple, Union

from jsonpath_ng.ext import parse

from .exception import JsonPathMissingRootError, JsonPathParseError, JSONNotFoundError


Policy = Dict[str, Any]
# TODO: not the precise definition of a key
Key = Tuple[str, str, Union[str, int, bytearray]]


class DocumentClient:
    """Client to run JSON queries"""

    def __init__(self, client: Client):
        self.client = client

    def get(self, key: Key, binName: str, jsonPath: str, readPolicy: Policy = None) -> Any:
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
        :raises: :exc:`~documentapi.exception.JsonPathMissingRootError`
        :raises: :exc:`~documentapi.exception.JsonPathParseError`
        :raises: :exc:`~documentapi.exception.JSONNotFoundError`
        """
        jsonPath = preprocessJsonPath(jsonPath)

        jsonPath = checkSyntax(jsonPath)

        jsonPath, advancedJsonPath = divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = buildContextArray(tokens)

        operatePolicy = convertToOperatePolicy(readPolicy)

        getOp = createGetOperation(binName, ctxs, lastToken)
        fetchedDocument = getSmallestDocument(self.client, key, binName, getOp, operatePolicy, jsonPath)

        # Use JSONPath library to perform advanced ops on fetched document
        if advancedJsonPath:
            jsonPathExpr = parse(advancedJsonPath)
            fetchedDocument = [match.value for match in jsonPathExpr.find(fetchedDocument)]

            # Check if an advanced operation other than length() exists
            # Other advanced operations yield a list of results
            # length() should yield an integer if called at the end
            # and the path has no other advanced ops
            matches = [re.search(op, advancedJsonPath) for op in ADVANCED_OP_TOKENS]
            matches = filter(lambda match: match is not None, matches)
            matches = list(matches)

            containsOtherAdvancedOps = False
            for match in matches:
                if match.re is not re.compile(r"\.`len`"):
                    containsOtherAdvancedOps = True

            if advancedJsonPath.endswith(".`len`") and containsOtherAdvancedOps is False:
                fetchedDocument = fetchedDocument[0]

        return fetchedDocument

    def put(self, key: Key, binName: str, jsonPath: str, obj: Any, writePolicy: Policy = None):
        """
        Put an object into a JSON document using JSON path.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path location to store the object
        :param dict writePolicy: the write policy for put() operation

        :raises: :exc:`~documentapi.exception.JsonPathMissingRootError`
        :raises: :exc:`~documentapi.exception.JsonPathParseError`
        :raises: :exc:`~documentapi.exception.JSONNotFoundError`
        """
        jsonPath = preprocessJsonPath(jsonPath)

        jsonPath = checkSyntax(jsonPath)

        jsonPath, advancedJsonPath = divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = buildContextArray(tokens)

        operatePolicy = convertToOperatePolicy(writePolicy)

        if advancedJsonPath:
            # If an advanced operation exists in JSON path
            # We need to fetch the whole document before it
            # and modify the document
            getOp = createGetOperation(binName, ctxs, lastToken)
            smallestDocument = getSmallestDocument(self.client, key, binName, getOp, operatePolicy, jsonPath)

            # Update smallest document
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.update(smallestDocument, obj)
        else:
            smallestDocument = obj

        # Send updated document to server
        putOp = createPutOperation(binName, ctxs, lastToken, smallestDocument)
        sendSmallestDocument(self.client, key, putOp, operatePolicy, jsonPath)

    def append(self, key: Key, binName: str, jsonPath: str, obj: Any, writePolicy: Policy = None):
        """
        Append an object to a list in a JSON document using JSON path.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path ending with a list
        :param dict writePolicy: the write policy for operate() operation

        :raises: :exc:`~documentapi.exception.JsonPathMissingRootError`
        :raises: :exc:`~documentapi.exception.JsonPathParseError`
        :raises: :exc:`~documentapi.exception.JSONNotFoundError`
        """
        jsonPath = preprocessJsonPath(jsonPath)

        jsonPath = checkSyntax(jsonPath)

        jsonPath, advancedJsonPath = divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = buildContextArray(tokens)

        op = createGetOperation(binName, ctxs, lastToken)

        # Get document from server
        operatePolicy = convertToOperatePolicy(writePolicy)
        smallestDocument = getSmallestDocument(self.client, key, binName, op, operatePolicy, jsonPath)

        if advancedJsonPath:
            # Append object to all matching lists
            jsonPathExpr = parse(advancedJsonPath)
            matches = jsonPathExpr.find(smallestDocument)
            for match in matches:
                match.value.append(obj)
        else:
            # List is the whole document
            smallestDocument.append(obj)

        # Send new document to server
        op = createPutOperation(binName, ctxs, lastToken, smallestDocument)
        sendSmallestDocument(self.client, key, op, operatePolicy, jsonPath)

    def delete(self, key: Key, binName: str, jsonPath: str, writePolicy: Policy = None):
        """
        Delete an object in a JSON document using JSON path.

        Deleting the root element causes the bin to contain an empty dictionary.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path of object to delete
        :param dict writePolicy: the write policy for operate() operation

        :raises: :exc:`~documentapi.exception.JsonPathMissingRootError`
        :raises: :exc:`~documentapi.exception.JsonPathParseError`
        :raises: :exc:`~documentapi.exception.JSONNotFoundError`
        """
        jsonPath = preprocessJsonPath(jsonPath)

        jsonPath = checkSyntax(jsonPath)

        jsonPath, advancedJsonPath = divideJsonPath(jsonPath)

        # Split up JSON path into tokens
        tokens = tokenize(jsonPath)

        # Then use tokens to build context arrays
        # except the last token
        lastToken = tokens.pop()
        ctxs = buildContextArray(tokens)

        operatePolicy = convertToOperatePolicy(writePolicy)

        if advancedJsonPath:
            # Fetch document from server
            op = createGetOperation(binName, ctxs, lastToken)
            smallestDocument = getSmallestDocument(self.client, key, binName, op, operatePolicy, jsonPath)

            # Delete all matches
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.filter(lambda _: True, smallestDocument)

            # Send new document to server
            op = createPutOperation(binName, ctxs, lastToken, smallestDocument)
            sendSmallestDocument(self.client, key, op, operatePolicy, jsonPath)
            return

        # Delete entire matched item
        # Create delete operation
        if type(lastToken) == int:
            op = list_operations.list_pop(binName, lastToken, ctx=ctxs)
        elif lastToken == "$":
            # Replace bin with empty dict
            op = operations.write(binName, {})
        else:
            op = map_operations.map_remove_by_key(binName, lastToken, MAP_RETURN_NONE, ctx=ctxs)

        # Tada
        try:
            # Deleting a missing key does not throw an error
            # User must check that it exists beforehand
            self.client.operate(key, [op], operatePolicy)
        except (ex.OpNotApplicable, ex.InvalidRequest):
            # OpNotApplicable: deleting from missing list/map, out of bounds index
            # InvalidRequest: deleting index from map or key from list
            raise JSONNotFoundError(jsonPath)


ADVANCED_OP_TOKENS = (
    r"\[\*\]",                              # [*]
    r"\.\.",                                # ..
    r"\[\?",                                # [?
    # Also check for negative indices
    r"\[(-?\d+)?\:(-?\d+)?(\:-?\d+)?\]",    # start:(end)?(:step)?
    r"\[-?\d+(\,-?\d+)+\]",                 # [idx1,idx2,...]
    r"\.`len`"                              # .`len`
)

# Helper functions for the api module


def preprocessJsonPath(jsonPath: str) -> str:
    # Replace any .length() calls with .`len`
    # Our JSONPath library only processes the latter
    jsonPath = re.sub(r"\.length\(\)", ".`len`", jsonPath)
    return jsonPath


def checkSyntax(jsonPath: str) -> str:
    # JSON path must start at document root
    if not jsonPath or jsonPath.startswith("$") is False:
        raise JsonPathMissingRootError(jsonPath)

    # Check for syntax errors
    try:
        parse(jsonPath)
    except Exception:
        raise JsonPathParseError(jsonPath)

    return jsonPath


# Divide JSON path into two parts
# The first part does not have advanced operations
# The second part starts with the first advanced operation in the path
def divideJsonPath(jsonPath: str) -> Tuple[str, Union[str, None]]:
    # Get substring in path beginning with the first advanced operation
    # Look for operations in path
    matches = [re.search(op, jsonPath) for op in ADVANCED_OP_TOKENS]
    # Filter out operations not in path
    matches = filter(lambda match: match is not None, matches)
    matches = list(matches)
    if matches:
        # If any operations exist, get the starting point of the first one
        # for the JSONPath library to process
        startIndices = map(lambda match: match.span()[0], matches)
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
def tokenize(firstJsonPath: str) -> List[str]:
    tokens = []
    token = ""
    quoteCount = 0

    for c in firstJsonPath:
        if c in ".[]" and quoteCount % 2 == 0:
            # Finished reading token
            # We know if the token is a finished quote if every quotation has a match
            if token == "":
                # No token found
                continue

            if c == ']':
                # Bracket notation
                if quoteCount > 0:
                    # Just finished reading a map key
                    token = token[1:-1]
                    quoteCount = 0
                else:
                    # Assume we read an index
                    token = int(token)

            # Save token
            tokens.append(token)
            token = ""
        else:
            if c == '\'':
                quoteCount += 1
            token = token + c

    # Flush out the rest of the buffer into last token
    # This is a map access (in dot notation)
    if token:
        tokens.append(token)

    # Special case: path ending with * returns the previous object
    if tokens[-1] == '*':
        tokens.pop()

    return tokens


def buildContextArray(tokens: List[str]) -> Union[List[str], None]:
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


def createGetOperation(binName: str, ctxs: list, lastToken: str) -> dict:
    # Create get operation using last token
    if type(lastToken) == int:
        op = list_operations.list_get_by_index(binName, lastToken, LIST_RETURN_VALUE, ctxs)
    elif lastToken == "$":
        # Get whole document
        op = operations.read(binName)
    else:
        op = map_operations.map_get_by_key(binName, lastToken, MAP_RETURN_VALUE, ctxs)

    return op


def createPutOperation(binName: str, ctxs: list, lastToken: str, obj: Any) -> dict:
    # Create put operation
    if type(lastToken) == int:
        op = list_operations.list_set(binName, lastToken, obj, ctx=ctxs)
    elif lastToken == "$":
        # Get whole document
        op = operations.write(binName, obj)
    else:
        op = map_operations.map_put(binName, lastToken, obj, ctx=ctxs)

    return op


OPERATE_CONFIG_KEYS = (
    "max_retries", "sleep_between_retries", "socket_timeout", "total_timeout", "compress", "key", "gen", "replica",
    "commit_level", "read_mode_ap", "read_mode_sc", "exists", "durable_delete", "expressions"
)


def convertToOperatePolicy(policy: Policy) -> Union[Policy, None]:
    if policy is None:
        return None

    # Filter out non-operate policies
    operatePolicy = policy.copy()
    for key in operatePolicy:
        if key not in OPERATE_CONFIG_KEYS:
            operatePolicy.pop(key)

    return operatePolicy

# These functions handle possible errors from calling operate()
# Pass in JSON path in case we throw an error


def getSmallestDocument(client: Client, key: Key, binName: str, op: Any, operatePolicy: Policy, jsonPath: str):
    try:
        _, _, bins = client.operate(key, [op], operatePolicy)
        fetchedDocument = bins[binName]
        if fetchedDocument is None:
            # Caused by using a key that doesn't exist in a map
            raise JSONNotFoundError(jsonPath)
    except (ex.BinIncompatibleType, ex.InvalidRequest, ex.OpNotApplicable):
        # InvalidRequest: index get() on a map or primitive
        # BinIncompatibleType: key get() on a list or primitive
        # OpNotApplicable: get() from missing list/map or out of bounds index
        raise JSONNotFoundError(jsonPath)
    return fetchedDocument


def sendSmallestDocument(client: Client, key: Key, op, operatePolicy: Policy, jsonPath: str):
    try:
        _, _, _ = client.operate(key, [op], operatePolicy)
    except ex.OpNotApplicable:
        # OpNotApplicable:
        # - put() into map as list
        # - put() into list as map
        # - put() into missing list/map
        raise JSONNotFoundError(jsonPath)
