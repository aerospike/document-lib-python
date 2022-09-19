import aerospike
from documentapi import DocumentClient
from documentapi.exception import JSONNotFoundError

# Configure Aerospike client to connect to Aerospike server
config = {
    "hosts": [("127.0.0.1", 3000)]
}
client = aerospike.Client(config)
client.connect()

# Store a JSON document in the form of lists and maps
key = ("test", "demo", "documentKey")
jsonDocument = {
    "key1": 40,
    "key2": [2, 4, 10, 20]
}
client.put(key, {"documentBin": jsonDocument})

# Use document client to access the document

# A document client acts as an adapter for the normal client
# to perform operations on JSON documents using JSONPath queries
documentClient = DocumentClient(client)

results = documentClient.get(key, "documentBin", "$.key2[1]")
print(results)
# 4

# Checking for exceptions
try:
    documentClient.get(key, "documentBin", "$.key3")
except JSONNotFoundError as e:
    print("Error:", e)
# Error: Unable to access document object with JSON path $.key3

# Cleanup
client.remove(key)
client.close()
