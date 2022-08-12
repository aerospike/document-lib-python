import unittest

# These JSONPaths should retrieve document content
# First order elements
# $
# $.key1
# $.key1[i]

# Second order elements
# $.key1.key2
# $.key1.key2[i]
# $.key1[i].key2
# $.key1[i].key2[j]

# Third order elements
# $.key1.key2.key3
# $.key1.key2.key3[i]
# $.key1.key2[i].key3
# $.key1.key2[i].key3[j]
# $.key1[i].key2.key3
# $.key1[i].key2.key3[j]
# $.key1[i].key2[j].key3
# $.key1[i].key2[j].key3[k]
# $.key1.key2[i][j]

from unittest.mock import MagicMock
from documentapi import DocumentClient
import json

class TestCorrectGets(unittest.TestCase):
    def setUp(self):
        # Refer to JSON document object as the correct answer
        jsonFile = open("testMap.json")
        self.correctJsonObj = json.load(jsonFile)

        # Mimic client behavior
        # Assume this client is connected to an Aerospike server
        # and the server has the JSON document encoded as a string
        client = MagicMock()
        client.select.return_value = (
            ('test', 'demo', None, bytearray(b';\xd4u\xbd\x0cs\xf2\x10\xb6~\xa87\x930\x0e\xea\xe5v(]')),
            {'ttl': 2591990, 'gen': 1},
            {'bin': json.dumps(self.correctJsonObj)}
        )

        self.documentClient = DocumentClient(client)
        self.keyTuple = ('test', 'demo', 'key')

    def tearDown(self):
        pass

    def testGetRoot(self):
        results = self.documentClient.get(self.keyTuple, "bin", "$")
        self.assertEqual(results, self.correctJsonObj)

    # First order elements
    
    def testGetMap(self):
        results = self.documentClient.get(self.keyTuple, "bin", "$.movies")
        self.assertEqual(results, self.correctJsonObj["movies"])

    def testGetList(self):
        pass
        # results = self.documentClient.get(self.keyTuple, "bin", "$[0]")
        # self.assertEquals(results, self.correctJsonObj[0])

    # Second order elements

    def testGetTwoMaps(self):
        pass

    def testGetMapThenList(self):
        results = self.documentClient.get(self.keyTuple, "bin", '$.movies[0]')
        self.assertEqual(results, self.correctJsonObj["movies"][0])

    def testGetListThenMap(self):
        pass

    def testGetTwoLists(self):
        pass

    # Third order elements

    def testGetThreeMaps(self):
        pass

    def testGetTwoMapsThenList(self):
        results = self.documentClient.get(self.keyTuple, "bin", "$.movies[0][viewers]")
        self.assertEqual(results, self.correctJsonObj["movies"][0]["viewers"])

    def testGetMapListMap(self):
        pass

    def testGetMapThenTwoLists(self):
        pass

    def testGetListThenTwoMaps(self):
        pass

    def testGetListMapList(self):
        pass

    def testGetTwoListsThenMap(self):
        pass

    def testGetThreeLists(self):
        pass

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
