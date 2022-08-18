from enum import unique
import aerospike
from aerospike import Client

from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations

from jsonpath_ng import jsonpath, parse

import re

from typing import Any

class DocumentClient:
    """Client to run JSON queries"""

    def __init__(self, client: Client):
        self.client = client

    def get(self, key: tuple, binName: str, jsonPath: str, readPolicy: dict = None) -> Any:
        """
        Get object(s) from a JSON document using JSON path.

        If multiple objects are matched, they will be returned as a :class:`list`.
        Otherwise, the object itself is returned.

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path to retrieve the object
        :param dict readPolicy: the read policy for get() operation

        :return: :py:obj:`Any`
        :raises: :exc:`KeyNotFound`
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

        # Remove keys from read policy that aren't in operate policy
        readPolicy = self.convertToOperatePolicy(readPolicy)

        # Fetch document
        _, _, bins = self.client.operate(key, [op], readPolicy)
        results = bins[binName]

        # Use JSONPath library to perform advanced ops on fetched document
        if advancedJsonPath:
            jsonPathExpr = parse(advancedJsonPath)
            results = [match.value for match in jsonPathExpr.find(results)]

        return results

    def put(self, key: tuple, binName: str, jsonPath: str, obj: Any, writePolicy: dict = None):
        """
        Put an object into a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path location to store the object
        :param dict writePolicy: the write policy for put() operation
        
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

        # Remove keys from write policy that aren't in operate policy
        writePolicy = self.convertToOperatePolicy(writePolicy)

        # Fetch smallest document
        _, _, bins = self.client.operate(key, [op], writePolicy)
        results = bins[binName]

        # Use JSONPath library to replace matches in fetched document
        if advancedJsonPath:
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.update(results, obj)
        else:
            # Just replace the whole fetched document
            results = obj
        
        # Send updated document to server
        op = self.createPutOperation(binName, ctxs, lastToken, obj)
        self.client.operate(key, [op], writePolicy)

    def append(self, key: tuple, binName: str, jsonPath: str, obj, writePolicy: dict = None):
        """
        Append an object to a list in a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path ending with a list
        :param dict writePolicy: the write policy for operate() operation
        
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

        # Remove keys from read policy that aren't in operate policy
        writePolicy = self.convertToOperatePolicy(writePolicy)

        # Fetch document
        _, _, bins = self.client.operate(key, [op], writePolicy)
        fetchedDocument = bins[binName]

        # Use JSONPath library to perform advanced ops on fetched document
        if advancedJsonPath:
            # Append object to all matching lists
            # TODO: add error handling
            jsonPathExpr = parse(advancedJsonPath)
            matches = jsonPathExpr.find(fetchedDocument)
            for match in matches:
                match.value.append(obj)
                match.full_path.update(match.value)
        else:
            # List is the whole document
            fetchedDocument.append(obj)

        # Send new document to server
        op = self.createPutOperation(binName, ctxs, lastToken, fetchedDocument)
        self.client.operate(key, [op], writePolicy)

    def delete(self, key: tuple, binName: str, jsonPath: str):
        """
        Delete an object in a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path of object to delete
        :param dict writePolicy: the write policy for operate() operation

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

        # Remove keys from read policy that aren't in operate policy
        writePolicy = self.convertToOperatePolicy(writePolicy)

        # Fetch document
        _, _, bins = self.client.operate(key, [op], writePolicy)
        fetchedDocument = bins[binName]

        # Use JSONPath library to perform advanced ops on fetched document
        if advancedJsonPath:
            # Delete object from all matches
            jsonPathExpr = parse(advancedJsonPath)
            jsonPathExpr.filter(fetchedDocument)

        # Send new document to server
        op = self.createPutOperation(binName, ctxs, lastToken)
        self.client.operate(key, [op], writePolicy)

    # Helper functions

    @staticmethod
    def validateJsonPath(jsonPath):
        # JSON path must start at document root
        if jsonPath and jsonPath.startswith("$") == False:
            raise ValueError("Invalid JSON path")

        # Check for syntax errors
        try:
            parse(jsonPath)
        except Exception:
            raise ValueError("Invalid JSON path")

    # Divide JSON path into two parts
    # The first part does not have advanced operations
    # The second part starts with the first advanced operation in the path
    @staticmethod
    def divideJsonPath(jsonPath):
        # Get substring in path beginning with the first advanced operation
        advancedOps = ["[*]", "..", "[?"]
        # Look for operations in path
        startIndices = [jsonPath.find(op) for op in advancedOps]
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

    # Split up a valid JSON path into map and list access tokens
    @staticmethod
    def tokenize(jsonPath):
        # First divide JSON path into "big" tokens
        # using map separator "."
        # Example:
        # "$[1].b.c[2]" -> ["$[1]", "b", "c[2]]"
        bigTokens = jsonPath.split(".")

        # Then divide each big token into "small" tokens
        # using list separator "[<index>]"
        # Example:
        # [$[1], b, c[2]] -> ["$", 1, "b", "c", 2]
        results = []
        for bigToken in bigTokens:
            smallTokens = re.split("\[|\]", bigToken)
            # Remove empty small tokens
            while "" in smallTokens:
                smallTokens.remove("")
    
            # First small token is always a map access or $
            # Every small token after it is a list access
            foundMapAccessOrRoot = False
            for smallToken in smallTokens:
                if foundMapAccessOrRoot:
                    # Encode list access token as an integer
                    smallToken = int(smallToken)
                else:
                    # Encode map access token as a string
                    foundMapAccessOrRoot = True

                results.append(smallToken)
        return results

    @staticmethod
    def buildContextArray(tokens):
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
        return ctxs

    @staticmethod
    def createGetOperation(binName, ctxs, lastToken):
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
    def createPutOperation(binName, ctxs, lastToken, obj):
        # Create put operation
        # TODO: list and map operations must be configured properly
        if type(lastToken) == int:
            op = list_operations.list_set(binName, lastToken, obj, ctx=ctxs)
        elif lastToken == "$":
            # Get whole document
            op = operations.write(binName, obj)
        else:
            op = map_operations.map_put(binName, lastToken, obj, ctx=ctxs)

        return op

    @staticmethod
    def convertToOperatePolicy(policy):
        operatePolicy = None
        if policy == None:
            return None
        
        # Filter out non-operate policies
        operatePolicy = policy.copy()
        operateKeys = [
            "max_retries", "sleep_between_retries", "socket_timeout", "total_timeout", "compress", "key", "gen", "replica",
            "commit_level", "read_mode_ap", "read_mode_sc", "exists", "durable_delete", "expressions"
        ]
        for key in operatePolicy:
            if key not in operateKeys:
                operatePolicy.pop(key)
