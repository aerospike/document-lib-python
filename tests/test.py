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

def setUpModule():
    # Need to access these variables across all test cases
    global mapJsonFile, listJsonFile
    global mapJsonObj, listJsonObj
    global listBinName, mapBinName
    global client, documentClient, keyTuple

    # Open JSON test files
    mapJsonFile = open("testMap.json")
    listJsonFile = open("testList.json")
    # Parse them into Python objects
    mapJsonObj = json.load(mapJsonFile)
    listJsonObj = json.load(listJsonFile)
    # Bins to insert JSON documents
    listBinName = "listBinName"
    mapBinName = "mapBinName"

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

class TestCorrectGets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Insert record with two bins
        # Each bin contains a JSON document
        client.put(keyTuple, {listBinName: listJsonObj})
        client.put(keyTuple, {mapBinName: mapJsonObj})

    @classmethod
    def tearDownClass(cls):
        # Remove record with two documents
        client.remove(keyTuple)

    def testGetRoot(self):
        results = documentClient.get(keyTuple, mapBinName, "$")
        self.assertEqual(results, mapJsonObj)

    # First order elements
    
    def testGetKey(self):
        results = documentClient.get(keyTuple, mapBinName, "$.map")
        self.assertEqual(results, mapJsonObj["map"])

    def testGetIndex(self):
        results = documentClient.get(keyTuple, listBinName, "$[0]")
        self.assertEqual(results, listJsonObj[0])

    # Second order elements

    def testGetTwoKeys(self):
        results = documentClient.get(keyTuple, mapBinName, "$.map.map")
        self.assertEqual(results, mapJsonObj["map"]["map"])

    def testGetKeyThenIndex(self):
        results = documentClient.get(keyTuple, mapBinName, '$.list[0]')
        self.assertEqual(results, mapJsonObj["list"][0])

    def testGetIndexThenKey(self):
        results = documentClient.get(keyTuple, listBinName, '$[0].map')
        self.assertEqual(results, listJsonObj[0]["map"])

    def testGetTwoIndices(self):
        results = documentClient.get(keyTuple, listBinName, '$[1][0]')
        self.assertEqual(results, listJsonObj[1][0])

    # Third order elements

    def testGetThreeKeys(self):
        results = documentClient.get(keyTuple, mapBinName, "$.map.map.int")
        self.assertEqual(results, mapJsonObj["map"]["map"]["int"])

    def testGetTwoKeysThenIndex(self):
        results = documentClient.get(keyTuple, mapBinName, "$.map.list[0]")
        self.assertEqual(results, mapJsonObj["map"]["list"][0])

    def testGetKeyIndexKey(self):
        results = documentClient.get(keyTuple, mapBinName, "$.list[0].int")
        self.assertEqual(results, mapJsonObj["list"][0]["int"])

    def testGetKeyThenTwoIndices(self):
        results = documentClient.get(keyTuple, mapBinName, "$.list[1][0]")
        self.assertEqual(results, mapJsonObj["list"][1][0])

    def testGetIndexThenTwoKeys(self):
        results = documentClient.get(keyTuple, listBinName, "$[0].map.int")
        self.assertEqual(results, listJsonObj[0]["map"]["int"])

    def testGetIndexKeyIndex(self):
        results = documentClient.get(keyTuple, listBinName, "$[0].list[0]")
        self.assertEqual(results, listJsonObj[0]["list"][0])

    def testGetTwoIndicesThenKey(self):
        results = documentClient.get(keyTuple, listBinName, "$[1][0].int")
        self.assertEqual(results, listJsonObj[1][0]["int"])

    def testGetThreeIndices(self):
        results = documentClient.get(keyTuple, listBinName, "$[1][1][0]")
        self.assertEqual(results, listJsonObj[1][1][0])

    # Wildstar tests

    def testGetWildstarIndex(self):
        results = documentClient.get(keyTuple, listBinName, "$[*]")
        self.assertEqual(results, listJsonObj)

    def testGetWildstarKey(self):
        results = documentClient.get(keyTuple, mapBinName, "$.*")
        expected = [mapJsonObj["map"], mapJsonObj["list"]]
        self.assertEqual(results, expected)

    def testGetNestedWildstarIndex(self):
        results = documentClient.get(keyTuple, listBinName, "$[1][*]")
        expected = listJsonObj[1]
        self.assertEqual(results, expected)

    def testGetNestedWildstarKey(self):
        results = documentClient.get(keyTuple, mapBinName, "$.map.*")
        expected = list(mapJsonObj["map"].values())
        self.assertEqual(results, expected)

    # Syntax errors

    def testGetEmpty(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "")

    def testGetMissingRoot(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "list")

    def testGetTrailingPeriod(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.")

    def testGetTrailingOpeningBracket(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list[")

    def testGetEmptyBrackets(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list[]")

    def testGetUnmatchedClosingBracket(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list]")

    # Access errors

    def testGetIndexFromMap(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.map[0]")

    def testGetKeyFromList(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list.nonExistentKey")

    def testGetIndexFromPrimitive(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list[0].int[0]")

    def testGetKeyFromPrimitive(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list[0].int.nonExistentKey")

    def testGetFromMissingMap(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.map.nonExistentMap.item")

    def testGetFromMissingList(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.map.nonExistentList[0]")

    def testGetOutOfBoundsIndex(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.list[1000]")
    
    def testGetMissingKey(self):
        self.assertRaises(ValueError, documentClient.get, keyTuple, mapBinName, "$.map.nonExistentKey")

class TestCorrectPuts(unittest.TestCase):
    def setUp(self):
        client.put(keyTuple, {mapBinName: mapJsonObj, listBinName: listJsonObj})

    def tearDown(self):
        client.remove(keyTuple)

    def testPutIntoMap(self):
        documentClient.put(keyTuple, mapBinName, "$.map.item", "hi")
        results = documentClient.get(keyTuple, mapBinName, "$.map.item")
        self.assertEqual(results, "hi")

    def testPutExistingListItem(self):
        documentClient.put(keyTuple, mapBinName, "$.list[0]", 2)
        results = documentClient.get(keyTuple, mapBinName, "$.list[0]")
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
        client.put(keyTuple, {mapBinName: mapJsonObj, listBinName: listJsonObj})

    def tearDown(self):
        client.remove(keyTuple)

    def testAppendIndexAccess(self):
        documentClient.append(keyTuple, mapBinName, "$.list[1]", 50)
        results = documentClient.get(keyTuple, mapBinName, "$.list[1]")
        self.assertEqual(results, [1, 50])

    def testAppendKeyAccess(self):
        documentClient.append(keyTuple, mapBinName, "$.list", 42)
        results = documentClient.get(keyTuple, mapBinName, "$.list")
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
        pass

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
