from nexus_python.nexusdb import NexusDB
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def main():
    nexus_db = NexusDB()

    # Step 1: Create a new relation
    relation_name = "example_relation2"
    columns = [
        {"name": "id"},
        {"name": "name"},
    ]
    create_response = nexus_db.create(relation_name, columns)
    print("Create relation response:", create_response)

    # Step 2: Insert data into the relation
    fields = ["id", "name"]
    values = [[1, "Item 1"], [2, "Item 2"]]
    insert_response = nexus_db.insert(relation_name, fields, values)
    print("Insert data response:", insert_response)

    # Step 3: Delete the data based on a primary key
    primary_keys = {"id": 1}
    delete_response = nexus_db.delete(relation_name, primary_keys)
    print("Delete data response:", delete_response)

    # Optional: Lookup to verify deletion (not implemented in the provided class, adjust as needed)
    lookup_response = nexus_db.lookup(relation_name, tabulate=True)
    print("Lookup after deletion:\n", lookup_response)


if __name__ == "__main__":
    main()
