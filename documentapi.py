import aerospike
from aerospike import Client

from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations

import re

from typing import Any

class DocumentClient:
    """Client to run JSON queries"""

    def __init__(self, client: Client):
        self.client = client

    # Assume JSON path is valid
    # Split up JSON path into map and list access tokens
    def tokenize(self, jsonPath):
        # First divide JSON path into "big" tokens
        # using map separator "."
        # Example:
        # "a[1].b.c[2]" -> ["a[1]", "b", "c[2]]"
        bigTokens = jsonPath.split(".")

        # Then divide each big token into "small" tokens
        # using list separator "[<index>]"
        # Example:
        # "[a[1], b, c[2]]" -> ["a", 1, "b", "c", 2]
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
                    if smallToken == "$":
                        # Don't treat root as a map access
                        continue

                results.append(smallToken)
        return results

    def buildContextArray(self, tokens):
        # Build context array
        ctxs = []
        for token in tokens:
            if type(token) == int:
                # List access
                ctx = cdt_ctx.cdt_ctx_list_index(token)
            else:
                # Map access
                ctx = cdt_ctx.cdt_ctx_map_key(token)
            ctxs.append(ctx)
        return ctxs

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
        # JSON path cannot be empty
        if len(jsonPath) == 0:
            return

        # Must start with $
        if jsonPath[0] != "$":
            return

        # Find the starting point of processing advanced queries
        # Advanced queries are processed by the client, not server
        clientSideOps = ["[*]", "..", "[?"]
        startIndex = min([jsonPath.find(op) for op in clientSideOps])

        if startIndex > 0:
            # Advanced queries found
            # Only get JSON document before that point
            # Save advanced query for processing later
            # advancedJsonPath = jsonPath[startIndex:]
            jsonPath = jsonPath[:startIndex]
            
        # Split up JSON path into tokens
        tokens = self.tokenize(jsonPath)

        # Then use tokens to build context arrays
        # But only before last access token
        lastToken = tokens.pop()
        ctxs = self.buildContextArray(tokens)

        # Get operation for last access token
        if type(lastToken) == int:
            # List access
            op = list_operations.list_get_by_index(binName, lastToken, aerospike.LIST_RETURN_VALUE, ctxs)
        else:
            # Map access
            op = map_operations.map_get_by_key(binName, lastToken, aerospike.MAP_RETURN_VALUE, ctxs)

        _, _, bins = self.client.operate(key, [op])
        results = bins[binName]
        return results

        # 2. Use JSONPath library on client side

        '''
        # Get bin containing JSON document
        _, _, bins = self.client.select(key, [binName], readPolicy)
        
        # Check if bin exists
        if binName not in bins:
            raise KeyError(f"Bin with name {binName} not found")
        
        # Parse the document into Python object
        document = bins[binName]
        jsonObj = json.loads(document)

        # Get matching objects
        expression = parse(jsonPath)
        results = [match.value for match in expression.find(jsonObj)]

        # Either return as list of multiple results or a single result
        if len(results) == 1:
            return results[0]
        else:
            return results
        '''

    def put(self, key: tuple, binName: str, jsonPath: str, obj: Any, writePolicy: dict = None):
        """
        Put an object into a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path location to store the object
        :param dict writePolicy: the write policy for put() operation
        
        """
        pass

    def append(self, key: tuple, binName: str, jsonPath: str, obj, writePolicy: dict = None):
        """
        Append an object to a list in a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path ending with a list
        :param dict writePolicy: the write policy for operate() operation
        
        """
        pass

    def delete(self, key: tuple, binName: str, jsonPath: str):
        """
        Delete an object in a JSON document using JSON path

        :param tuple key: the key of the record
        :param str binName: the name of the bin containing the JSON document
        :param str jsonPath: JSON path of object to delete
        :param dict writePolicy: the write policy for operate() operation

        """
        pass

if __name__ == "__main__":
    config = {
        'hosts': [("127.0.0.1", 3000)]
    }
    client = aerospike.client(config).connect()
    documentClient = DocumentClient(client)

    keyTuple = ("test", "demo", "key")

    jsonDocument = '{"array": [1, 2]}'
    client.put(keyTuple, {"bin": jsonDocument})
    documentClient.get(keyTuple, "bin", "$.array")

    client.truncate("test", None, 0)
