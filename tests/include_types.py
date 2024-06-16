from dotenv import load_dotenv

from nexus_python.nexusdb import NexusDB

# Load environment variables from .env file
load_dotenv()


def main():
    nexus_db = NexusDB()

    # Step 1: Create a new relation
    relation_name = "example_relation2"
    # columns = [
    #     {"name": "id"},
    #     {"name": "name"},
    # ]
    # create_response = nexus_db.create(relation_name, columns)
    # print("Create relation response:", create_response)

    # Step 2: Insert data into the relation
    fields = ["id", "name"]
    values = [[1, "Item 1"], [2, "Item 2"]]
    insert_response = nexus_db.insert(relation_name, fields, values)
    print("\nInsert data response:", insert_response)

    lookup_response = nexus_db.lookup(relation_name, tabulate=True, include_types=True)
    print("\ntabulate=True, include_types=True:\n", lookup_response)

    lookup_response = nexus_db.lookup(relation_name, tabulate=False, include_types=True)
    print("\ntabulate=False, include_types=True:\n", lookup_response)

    lookup_response = nexus_db.lookup(relation_name, tabulate=True, include_types=False)
    print("\ntabulate=True, include_types=False:\n", lookup_response)

    lookup_response = nexus_db.lookup(
        relation_name, tabulate=False, include_types=False
    )
    print("\ntabulate=False, include_types=False:\n", lookup_response)

    # Step 3: Delete the data based on a primary key
    condition = "id = 1"
    delete_response = nexus_db.delete(relation_name, condition)
    print("\n\nDelete data response:", delete_response)

    # Optional: Lookup to verify deletion (not implemented in the provided class, adjust as needed)
    lookup_response = nexus_db.lookup(relation_name, tabulate=True)
    print("Lookup after deletion:\n", lookup_response)


if __name__ == "__main__":
    main()
