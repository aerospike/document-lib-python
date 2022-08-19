import unittest

from unittest.mock import MagicMock
import json
import aerospike

# Must add parent directory to system path
# So we can import documentapi
import sys
import os

parentDirAbsPath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, parentDirAbsPath)

from documentapi import DocumentClient
from documentapiexception import *

# Bins to insert JSON documents
LIST_BIN_NAME = "testList"
MAP_BIN_NAME = "testMap"

def setUpModule():
    # Need to access these variables across all test cases
    global mapJsonFile, listJsonFile
    global mapJsonObj, listJsonObj
    global client, documentClient, keyTuple

    # Open JSON test files
    mapJsonFile = open("testMap.json")
    listJsonFile = open("testList.json")
    # Parse them into Python objects
    mapJsonObj = json.load(mapJsonFile)
    listJsonObj = json.load(listJsonFile)

    # Setup client
    config = {
        "hosts": [("127.0.0.1", 3000)]
    }
    client = aerospike.client(config).connect()
    documentClient = DocumentClient(client)

    # Record key for all tests
    keyTuple = ('test', 'demo', 'key')

def tearDownModule():
    # Close file descriptors
    mapJsonFile.close()
    listJsonFile.close()
    # Close client connection
    client.close()

class TestGets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Insert record with two bins
        # Each bin contains a JSON document
        client.put(keyTuple, {LIST_BIN_NAME: listJsonObj})
        client.put(keyTuple, {MAP_BIN_NAME: mapJsonObj})

    @classmethod
    def tearDownClass(cls):
        # Remove record with two documents
        client.remove(keyTuple)

class TestCorrectGets(TestGets):
    def testGetRoot(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, mapJsonObj)

    # First order elements
    
    def testGetKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map")
        self.assertEqual(results, mapJsonObj["map"])

    def testGetIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[0]")
        self.assertEqual(results, listJsonObj[0])

    # Second order elements

    def testGetTwoKeys(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.map")
        self.assertEqual(results, mapJsonObj["map"]["map"])

    def testGetKeyThenIndex(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, '$.list[0]')
        self.assertEqual(results, mapJsonObj["list"][0])

    def testGetIndexThenKey(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, '$[0].map')
        self.assertEqual(results, listJsonObj[0]["map"])

    def testGetTwoIndices(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, '$[1][0]')
        self.assertEqual(results, listJsonObj[1][0])

    # Third order elements

    def testGetThreeKeys(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.map.int")
        self.assertEqual(results, mapJsonObj["map"]["map"]["int"])

    def testGetTwoKeysThenIndex(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.list[0]")
        self.assertEqual(results, mapJsonObj["map"]["list"][0])

    def testGetKeyIndexKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[0].int")
        self.assertEqual(results, mapJsonObj["list"][0]["int"])

    def testGetKeyThenTwoIndices(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[1][0]")
        self.assertEqual(results, mapJsonObj["list"][1][0])

    def testGetIndexThenTwoKeys(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[0].map.int")
        self.assertEqual(results, listJsonObj[0]["map"]["int"])

    def testGetIndexKeyIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[0].list[0]")
        self.assertEqual(results, listJsonObj[0]["list"][0])

    def testGetTwoIndicesThenKey(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][0].int")
        self.assertEqual(results, listJsonObj[1][0]["int"])

    def testGetThreeIndices(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][1][0]")
        self.assertEqual(results, listJsonObj[1][1][0])

    # Wildstar tests

    def testGetWildstarIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[*]")
        self.assertEqual(results, listJsonObj)

    def testGetWildstarKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.*")
        expected = [mapJsonObj["map"], mapJsonObj["list"]]
        self.assertEqual(results, expected)

    def testGetNestedWildstarIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][*]")
        expected = listJsonObj[1]
        self.assertEqual(results, expected)

    def testGetNestedWildstarKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.*")
        expected = list(mapJsonObj["map"].values())
        self.assertEqual(results, expected)

class TestIncorrectGets(TestGets):
    # Syntax errors

    def testGetEmpty(self):
        self.assertRaises(JsonPathMissingRootError, documentClient.get, keyTuple, MAP_BIN_NAME, "")

    def testGetMissingRoot(self):
        self.assertRaises(JsonPathMissingRootError, documentClient.get, keyTuple, MAP_BIN_NAME, "list")

    def testGetTrailingPeriod(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.")

    def testGetTrailingOpeningBracket(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[")

    def testGetEmptyBrackets(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[]")

    def testGetUnmatchedClosingBracket(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list]")

    # Access errors

    def testGetIndexFromMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.map[0]")

    def testGetKeyFromList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list.nonExistentKey")

    def testGetIndexFromPrimitive(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[0].int[0]")

    def testGetKeyFromPrimitive(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[0].int.nonExistentKey")

    def testGetFromMissingMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.map.nonExistentMap.item")

    def testGetFromMissingList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.map.nonExistentList[0]")

    def testGetOutOfBoundsIndex(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[1000]")
    
    def testGetMissingKey(self):
        self.assertRaises(ObjectNotFoundError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.map.nonExistentKey")

class TestCorrectPuts(unittest.TestCase):
    def setUp(self):
        client.put(keyTuple, {MAP_BIN_NAME: mapJsonObj, LIST_BIN_NAME: listJsonObj})

    def tearDown(self):
        client.remove(keyTuple)

    def testPutIntoMap(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$.map.item", "hi")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.item")
        self.assertEqual(results, "hi")

    def testPutExistingListItem(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$.list[0]", 2)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[0]")
        self.assertEqual(results, 2)

class TestIncorrectPuts(unittest.TestCase):
    def testPutIntoMissingMap(self):
        pass

    def testPutIntoMissingList(self):
        pass

    def testPutIntoMapAsList(self):
        pass

    def testPutIntoListAsMap(self):
        pass

class TestCorrectAppend(unittest.TestCase):
    def setUp(self):
        client.put(keyTuple, {MAP_BIN_NAME: mapJsonObj, LIST_BIN_NAME: listJsonObj})

    def tearDown(self):
        client.remove(keyTuple)

    def testAppendIndexAccess(self):
        documentClient.append(keyTuple, MAP_BIN_NAME, "$.list[1]", 50)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[1]")
        self.assertEqual(results, [1, 50])

    def testAppendKeyAccess(self):
        documentClient.append(keyTuple, MAP_BIN_NAME, "$.list", 42)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list")
        expected = [{"int": 1}, [1], 42]
        self.assertEqual(results, expected)

class TestIncorrectAppend(unittest.TestCase):
    def testAppendMissingList(self):
        pass

    def testAppendMap(self):
        pass

    def testAppendPrimitive(self):
        pass

class TestCorrectDelete(unittest.TestCase):
    def testDeleteRoot(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        expected = [{"int": 1}, [1], 42]
        self.assertEqual(results, expected)

    def testDeletePrimitiveFromMap(self):
        pass

    def testDeletePrimitiveFromList(self):
        pass

    def testDeleteMapFromMap(self):
        pass

    def testDeleteListFromMap(self):
        pass

    def testDeleteMapFromList(self):
        pass

    def testDeleteListFromList(self):
        pass

class TestIncorrectDelete(unittest.TestCase):
    def testDeleteMissingKey(self):
        pass

    def testDeleteMissingIndex(self):
        pass

    def testDeleteKeyInList(self):
        pass

    def testDeleteIndexInMap(self):
        pass

    def testDeleteFromMissingMap(self):
        pass

    def testDeleteFromMissingList(self):
        pass

if __name__=="__main__":
    unittest.main()
