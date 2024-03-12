# nexus-python

Official Python wrapper for NexusDB API

install the package:

```sh
pip install nexusdb-python
```

Then, you can use the package to interact with the Nexus API:

**Hint: make sure to replace "your_api_key" with your actual API key, which is found in the [Nexus dashboard](https://www.nexusdb.io/dashboard/).**

```python
from nexus_python.nexusdb import NexusDB

nexus_db = NexusDB(api_key="your_api_key")

# Step 1: Create a new relation
relation_name = "example_relation"
columns = [
    {"name": "id", "type": "Int", "is_primary": True},
    {"name": "name", "type": "String"},
]
create_response = nexus_db.create(relation_name, columns)
print("Create relation response:", create_response)

# Step 2: Insert data into the relation
fields = ["id", "name"]
values = [[1, "Item 1"], [2, "Item 2"]]
insert_response = nexus_db.insert(relation_name, fields, values)
print("Insert data response:", insert_response)

# Step 3: Delete one line of data based on a primary key
primary_keys = {"id": 1}
delete_response = nexus_db.delete(relation_name, primary_keys)
print("Delete data response:", delete_response)

# Optional: Lookup to verify deletion
lookup_response = nexus_db.lookup(relation_name, tabulate=True)
print("Lookup after deletion:\n", lookup_response)
```
