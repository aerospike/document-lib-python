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

class TestCorrectGets(unittest.TestCase):
    def setUp(self):
        self.mapJsonFile = open("testMap.json")
        self.listJsonFile = open("testList.json")
        self.mapJsonObj = json.load(self.mapJsonFile)
        self.listJsonObj = json.load(self.listJsonFile)

        self.listBinName = "listBinName"
        self.mapBinName = "mapBinName"

        config = {
            "hosts": [("127.0.0.1", 3000)]
        }
        self.client = aerospike.client(config).connect()

        self.keyTuple = ('test', 'demo', 'key')
        self.client.put(self.keyTuple, {self.listBinName: self.listJsonObj})
        self.client.put(self.keyTuple, {self.mapBinName: self.mapJsonObj})

        # Mimic client behavior
        # Assume this client is connected to an Aerospike server
        # and the server has the JSON document encoded as nested maps and lists
        # client = MagicMock()
        # client.select.return_value = (
        #     ('test', 'demo', None, bytearray(b';\xd4u\xbd\x0cs\xf2\x10\xb6~\xa87\x930\x0e\xea\xe5v(]')),
        #     {'ttl': 2591990, 'gen': 1},
        #     {
        #         self.listBinName: self.listJsonObj,
        #         self.mapBinName: self.mapJsonObj
        #     }
        # )

        self.documentClient = DocumentClient(self.client)

    def tearDown(self):
        self.mapJsonFile.close()
        self.listJsonFile.close()
        self.client.close()

    def testGetRoot(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$")
        self.assertEqual(results, self.mapJsonObj)

    # First order elements
    
    def testGetKey(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.map")
        self.assertEqual(results, self.mapJsonObj["map"])

    def testGetIndex(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[0]")
        self.assertEqual(results, self.listJsonObj[0])

    # Second order elements

    def testGetTwoKeys(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.map.map")
        self.assertEqual(results, self.mapJsonObj["map"]["map"])

    def testGetKeyThenIndex(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, '$.list[0]')
        self.assertEqual(results, self.mapJsonObj["list"][0])

    def testGetIndexThenKey(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, '$[0].map')
        self.assertEqual(results, self.listJsonObj[0]["map"])

    def testGetTwoIndices(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, '$[1][0]')
        self.assertEqual(results, self.listJsonObj[1][0])

    # Third order elements

    def testGetThreeKeys(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.map.map.int")
        self.assertEqual(results, self.mapJsonObj["map"]["map"]["int"])

    def testGetTwoKeysThenIndex(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.map.list[0]")
        self.assertEqual(results, self.mapJsonObj["map"]["list"][0])

    def testGetKeyIndexKey(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.list[0].int")
        self.assertEqual(results, self.mapJsonObj["list"][0]["int"])

    def testGetKeyThenTwoIndices(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.list[1][0]")
        self.assertEqual(results, self.mapJsonObj["list"][1][0])

    def testGetIndexThenTwoKeys(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[0].map.int")
        self.assertEqual(results, self.listJsonObj[0]["map"]["int"])

    def testGetIndexKeyIndex(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[0].list[0]")
        self.assertEqual(results, self.listJsonObj[0]["list"][0])

    def testGetTwoIndicesThenKey(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[1][0].int")
        self.assertEqual(results, self.listJsonObj[1][0]["int"])

    def testGetThreeIndices(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[1][1][0]")
        self.assertEqual(results, self.listJsonObj[1][1][0])

    # Wildstar tests

    def testGetWildstarIndex(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[*]")
        self.assertEqual(results, self.listJsonObj)

    def testGetWildstarKey(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.*")
        expected = [self.mapJsonObj["map"], self.mapJsonObj["list"]]
        self.assertEqual(results, expected)

    def testGetNestedWildstarIndex(self):
        results = self.documentClient.get(self.keyTuple, self.listBinName, "$[1][*]")
        expected = self.listJsonObj[1]
        self.assertEqual(results, expected)

    def testGetNestedWildstarKey(self):
        results = self.documentClient.get(self.keyTuple, self.mapBinName, "$.map.*")
        expected = list(self.mapJsonObj["map"].values())
        self.assertEqual(results, expected)

    # Syntax errors

    def testGetEmpty(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "")

    def testGetMissingRoot(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "list")

    def testGetTrailingPeriod(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "$.")

    def testGetTrailingOpeningBracket(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "$.list[")

    def testGetEmptyBrackets(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "$.list[]")

    def testGetUnmatchedClosingBracket(self):
        self.assertRaises(ValueError, self.documentClient.get, self.keyTuple, self.mapBinName, "$.list]")

# Test incorrect paths

# Reference a list as if it were a map
# Reference a map as if it were a list
# Reference a primitive as if it was a map
# Reference a primitive as if it was a list
# Reference a list item that is not there (out of bounds)
# Reference a map that isn't there
# Reference a list that isn't there

class TestIncorrectGets(unittest.TestCase):

    def testGetListAsMap(self):
        pass

    def testGetMapAsList(self):
        pass

    def testGetPrimitiveAsMap(self):
        pass

    def testGetPrimitiveAsList(self):
        pass

    def testGetMissingMap(self):
        pass

    def testGetMissingList(self):
        pass

    def testGetMissingListItem(self):
        pass
    
    # TODO: add?
    def testGetMissingMapItem(self):
        pass

class TestCorrectPuts(unittest.TestCase):
    def testPutIntoMap(self):
        pass

    def testPutIntoList(self):
        pass

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
    def testAppendKeyedList(self):
        pass

    def testAppendIndexedList(self):
        pass

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
