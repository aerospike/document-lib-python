import sys
import os
import unittest
import copy
import json
import aerospike

from documentapi import DocumentClient
from documentapi.exception import JsonPathMissingRootError, JsonPathParseError, ObjectNotFoundError

# Bins to insert JSON documents
LIST_BIN_NAME = "testList"
MAP_BIN_NAME = "testMap"


def setUpModule():
    # Need to access these variables across all test cases
    global mapJsonFile, listJsonFile
    global mapJsonObj, listJsonObj
    global client, documentClient, keyTuple

    # Open JSON test files
    TEST_MAP_FILE_PATH = os.path.join(sys.path[0], "testMap.json")
    TEST_LIST_FILE_PATH = os.path.join(sys.path[0], "testList.json")
    mapJsonFile = open(TEST_MAP_FILE_PATH)
    listJsonFile = open(TEST_LIST_FILE_PATH)
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


# Helper function for all tests

def deleteJsonMapValuesRecursively(jsonObj, key=None):
    if type(jsonObj) == list:
        # Recurse through every element in list
        for element in jsonObj:
            deleteJsonMapValuesRecursively(element, key)

    if type(jsonObj) != dict:
        # Object cannot have any key-value pairs
        return

    # Delete all matching keys and recurse into remaining values
    keysToDelete = []
    for iteratedKey in jsonObj.keys():
        value = jsonObj[iteratedKey]
        if key is None or iteratedKey == key:
            # Matches
            keysToDelete.append(key)
        else:
            deleteJsonMapValuesRecursively(value, key)

    for key in keysToDelete:
        del jsonObj[key]


# Gets all values if a key is not provided
def getJsonMapValuesRecursively(jsonObj, key=None):
    if type(jsonObj) == list:
        # Recurse through every element in list
        values = []
        for element in jsonObj:
            elementValues = getJsonMapValuesRecursively(element, key)
            values.extend(elementValues)
        return values

    if type(jsonObj) != dict:
        # Object cannot have any key-value pairs
        return []

    # Add all matching keys and recurse into each value
    values = []
    for iteratedKey in jsonObj.keys():
        value = jsonObj[iteratedKey]
        if key is None or iteratedKey == key:
            # Matches
            values.append(value)
        nestedValues = getJsonMapValuesRecursively(value, key)
        values.extend(nestedValues)
    return values


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

    # Bracket notation tests

    def testGetOneKeyInBracket(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$['map']")
        self.assertEqual(results, mapJsonObj["map"])

    def testGetTwoKeysInBracket(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$['map']['map']")
        self.assertEqual(results, mapJsonObj["map"]["map"])

class TestGetAdvancedOps(TestGets):

    # get() may return multiple matches in any order
    # This function checks if the functions return the expected matches
    @staticmethod
    def isListEqualUnsorted(list1, list2):
        if len(list1) != len(list2):
            # Unequal lengths
            return False

        # Don't modify original lists to compare them
        list1 = copy.deepcopy(list1)
        list2 = copy.deepcopy(list2)

        while list2:
            element = list2.pop()
            if element not in list1:
                return False
            list1.remove(element)
        return len(list1) == 0

    # Wildstar index tests

    def testGetWildstarIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[*]")
        self.assertTrue(self.isListEqualUnsorted(results, listJsonObj))

    def testGetNestedWildstarIndex(self):
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][*]")
        expected = listJsonObj[1]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testGetWildstarIndexBeforeKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[*].int")
        # Get value in every dictionary
        expected = getJsonMapValuesRecursively(mapJsonObj["dictsWithSameField"], "int")

        self.assertTrue(self.isListEqualUnsorted(results, expected))

    # Wildstar key tests

    def testGetWildstarKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.*")
        self.assertEqual(results, mapJsonObj)

    def testGetNestedWildstarKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.*")
        self.assertEqual(results, mapJsonObj["map"])

    # Recursion Tests

    def testGetRecursiveKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField..int")

        # Get all field "int" values in a specific map
        expected = getJsonMapValuesRecursively(mapJsonObj["dictsWithSameField"], "int")
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testGetRecursiveFromRoot(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$..int")

        # Get all field "int" values in the entire bin document
        expected = getJsonMapValuesRecursively(mapJsonObj, "int")
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testGetRecursiveWildstarKey(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$..*")

        # Get all field values
        expected = getJsonMapValuesRecursively(mapJsonObj)
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testRecursiveBracket(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$..['int']")
        expected = getJsonMapValuesRecursively(mapJsonObj, "int")
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    # Filter tests

    def testFilterDictsWithInt(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.int)]")
        expected = mapJsonObj["dictsWithSameField"][:3]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testFilterLT(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.int > 10)]")
        expected = mapJsonObj["dictsWithSameField"][1:3]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testFilterAnd(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.int > 10 & @.int < 50)]")
        expected = mapJsonObj["dictsWithSameField"][1:3]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    def testFilterOr(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.int < 10 | @.int > 40)]")
        expected = [mapJsonObj["dictsWithSameField"][0], mapJsonObj["dictsWithSameField"][1]]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    @unittest.skip("Unsupported")
    def testFilterLTVar(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.int < $['compareVar'])]")
        expected = mapJsonObj["dictsWithSameField"][0]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    @unittest.skip("Unsupported")
    def testFilterRegex(self):
        # Matches anything ending with mesa and ignores case
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField[?(@.str =~ /.*mesa/i)]")
        expected = mapJsonObj["dictsWithSameField"][0]
        self.assertTrue(self.isListEqualUnsorted(results, expected))

    # Function tests

    def testLength(self):
        length = documentClient.get(keyTuple, MAP_BIN_NAME, "$.dictsWithSameField.length()")
        self.assertEqual(len(mapJsonObj["dictsWithSameField"]), length)

    @unittest.skip("Unaddressed bug")
    def testLengthInQuotes(self):
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$['.length()']")
        self.assertEqual(mapJsonObj[".length()"], results)

    def testWildstarLength(self):
        length = documentClient.get(keyTuple, LIST_BIN_NAME, "$[*].length()")
        expected = [len(element) for element in listJsonObj]
        self.assertEqual(expected, length)

    # Lists

    def testSlices(self):
        # [2, 4) -> [2, 3]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][2:4]")
        expected = listJsonObj[1][2:4]
        self.assertEqual(expected, results)

    def testSliceFromStart(self):
        # [_, 2) -> [0, 1]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][:2]")
        expected = listJsonObj[1][:2]
        self.assertEqual(expected, results)

    def testSliceFromEnd(self):
        # [2, end) -> [2, 3, ... last index]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][2:]")
        expected = listJsonObj[1][2:]
        self.assertEqual(expected, results)

    def testSliceFromLastIndex(self):
        # [-1, ) -> [last index]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][-1:]")
        expected = listJsonObj[1][-1:]
        self.assertEqual(expected, results)

    def testSliceToLastIndex(self):
        # [3, -1) -> [last index]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][3:-1]")
        expected = listJsonObj[1][3:-1]
        self.assertEqual(expected, results)

    def testSetOfIndices(self):
        # [3, 5]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][3,5]")
        expected = [listJsonObj[1][3], listJsonObj[1][5]]
        self.assertEqual(expected, results)

    def testSlicesWithStep(self):
        # [2, 5) step 1 -> [2, 4]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][2:5:2]")
        expected = listJsonObj[1][2:5:2]
        self.assertEqual(expected, results)

    def testSlicesWithEndAndStep(self):
        # [, 2) step 2 -> [0, 2]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][:4:2]")
        expected = listJsonObj[1][:4:2]
        self.assertEqual(expected, results)

    def testSlicesWithOnlyStep(self):
        # step 2 -> [0, 2, 4]
        results = documentClient.get(keyTuple, LIST_BIN_NAME, "$[1][::2]")
        expected = listJsonObj[1][::2]
        self.assertEqual(expected, results)

class TestIncorrectGets(TestGets):

    # Syntax errors

    def testGetEmpty(self):
        self.assertRaises(JsonPathMissingRootError, documentClient.get, keyTuple, MAP_BIN_NAME, "")

    def testGetMissingRoot(self):
        self.assertRaises(JsonPathMissingRootError, documentClient.get, keyTuple, MAP_BIN_NAME, "list")

    def testGetTrailingPeriod(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.")
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.asdf.")

    def testGetTrailingRecursive(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$..")

    def testGetTrailingOpeningBracket(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, LIST_BIN_NAME, "$[")
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[")

    def testGetEmptyBrackets(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, LIST_BIN_NAME, "$[]")
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, MAP_BIN_NAME, "$.list[]")

    def testGetUnmatchedClosingBracket(self):
        self.assertRaises(JsonPathParseError, documentClient.get, keyTuple, LIST_BIN_NAME, "$]")
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


class TestWrites(unittest.TestCase):
    def setUp(self):
        client.put(keyTuple, {MAP_BIN_NAME: mapJsonObj, LIST_BIN_NAME: listJsonObj})

    def tearDown(self):
        client.remove(keyTuple)


class TestCorrectPuts(TestWrites):

    # Inserting root document

    def testPutNewRootAsMap(self):
        # Override setup
        client.remove(keyTuple)

        documentClient.put(keyTuple, MAP_BIN_NAME, "$", {})
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, {})

    def testPutNewRootAsList(self):
        # Override setup
        client.remove(keyTuple)

        documentClient.put(keyTuple, MAP_BIN_NAME, "$", [])
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, [])

    def testReplaceRootWithMap(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$", {})
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, {})

    def testReplaceRootWithList(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$", [])
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, [])

    def testPutIntoMap(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$.map.item", "hi")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.item")
        self.assertEqual(results, "hi")

    def testPutExistingListItem(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$.list[0]", 2)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[0]")
        self.assertEqual(results, 2)


class TestPutsAdvancedOps(TestWrites):

    def testPutDeepScan(self):
        documentClient.put(keyTuple, MAP_BIN_NAME, "$..int", 99)

        # All ints should be 99
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        intValues = getJsonMapValuesRecursively(results, "int")
        areIntsAll99 = all([value == intValues[0] for value in intValues])

        self.assertTrue(areIntsAll99)


class TestIncorrectPuts(TestWrites):

    def testPutIntoMissingMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.put, keyTuple, MAP_BIN_NAME, "$.map.nonExistentMap.item", 4)

    def testPutIntoMissingList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.put, keyTuple, MAP_BIN_NAME, "$.map.nonExistentList[0]", 4)

    def testPutIntoMapAsList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.put, keyTuple, MAP_BIN_NAME, "$.map.nonExistentMap[0]", 4)

    def testPutIntoListAsMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.put, keyTuple, MAP_BIN_NAME, "$.map.nonExistentList.item", 4)


class TestCorrectAppend(TestWrites):

    def testAppendIndexAccess(self):
        documentClient.append(keyTuple, MAP_BIN_NAME, "$.list[1]", 50)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[1]")
        self.assertEqual(results, [1, 50])

    def testAppendKeyAccess(self):
        documentClient.append(keyTuple, MAP_BIN_NAME, "$.list", 42)
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list")
        expected = [{"int": 1}, [1], 42]
        self.assertEqual(results, expected)


class TestIncorrectAppend(TestWrites):

    def testAppendMissingList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.append, keyTuple, MAP_BIN_NAME, "$.map.nonExistentList", 4)

    # TODO: replace with custom errors?

    def testAppendMap(self):
        self.assertRaises(AttributeError, documentClient.append, keyTuple, MAP_BIN_NAME, "$.map", 4)

    def testAppendPrimitive(self):
        self.assertRaises(AttributeError, documentClient.append, keyTuple, MAP_BIN_NAME, "$.map.map.int", 4)


class TestCorrectDelete(TestWrites):

    def testDeleteRoot(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")
        self.assertEqual(results, {})

    def testDeletePrimitiveFromMap(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.map.map.int")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map.map")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["map"]["map"]["int"]

        self.assertEqual(results, expectedJsonObj["map"]["map"])

    def testDeletePrimitiveFromList(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.list[1][0]")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list[1]")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["list"][1][0]

        self.assertEqual(results, expectedJsonObj["list"][1])

    def testDeleteMapFromMap(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.map.map")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["map"]["map"]

        self.assertEqual(results, expectedJsonObj["map"])

    def testDeleteListFromMap(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.map.list")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.map")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["map"]["list"]

        self.assertEqual(results, expectedJsonObj["map"])

    def testDeleteMapFromList(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.list[0]")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["list"][0]

        self.assertEqual(results, expectedJsonObj["list"])

    def testDeleteListFromList(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.list[1]")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$.list")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        del expectedJsonObj["list"][1]

        self.assertEqual(results, expectedJsonObj["list"])


class TestDeleteAdvancedOps(TestWrites):

    def testDeepScanDelete(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$..int")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")

        expectedJsonObj = copy.deepcopy(mapJsonObj)
        deleteJsonMapValuesRecursively(expectedJsonObj, "int")

        self.assertEqual(results, expectedJsonObj)

    def testDeleteDeepScanWildstar(self):
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$..*")
        results = documentClient.get(keyTuple, MAP_BIN_NAME, "$")

        self.assertEqual(results, {})


class TestIncorrectDelete(TestWrites):

    def testDeleteMissingKey(self):
        # No exception will be raised
        documentClient.delete(keyTuple, MAP_BIN_NAME, "$.map.nonExistentKey")

    def testDeleteOutOfBoundsIndex(self):
        self.assertRaises(ObjectNotFoundError, documentClient.delete, keyTuple, MAP_BIN_NAME, "$.list[1000]")

    def testDeleteKeyInList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.delete, keyTuple, MAP_BIN_NAME, "$.list.nonExistentKey")

    def testDeleteIndexInMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.delete, keyTuple, MAP_BIN_NAME, "$.map[0]")

    def testDeleteFromMissingMap(self):
        self.assertRaises(ObjectNotFoundError, documentClient.delete, keyTuple, MAP_BIN_NAME, "$.map.nonExistentMap.item")

    def testDeleteFromMissingList(self):
        self.assertRaises(ObjectNotFoundError, documentClient.delete, keyTuple, MAP_BIN_NAME, "$.map.nonExistentList[0]")


if __name__ == "__main__":
    unittest.main()
