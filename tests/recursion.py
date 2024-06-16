import os

from dotenv import load_dotenv

from nexus_python.nexusdb import NexusDB

# Load environment variables from .env file
load_dotenv()


def main():
    nexus_db = NexusDB()

    # Step 1: Create the 'Graph' relation
    relation_name = "Graph"
    columns = [
        {"name": "sourceId"},
        {"name": "sourceName"},
        {"name": "access_keys", "type": "Array(String)?"},
        {"name": "owner"},
        {"name": "relationship"},
        {"name": "targetName"},
        {"name": "targetId"},
    ]
    create_response = nexus_db.create(relation_name, columns)
    print("Create relation response:", create_response)

    # Step 2: Insert data into the 'Graph' relation
    fields = [
        "sourceId",
        "sourceName",
        "access_keys",
        "owner",
        "relationship",
        "targetName",
        "targetId",
    ]
    values = [
        [
            "emailmessage_1",
            "Re: Poetry",
            None,
            "user_1234",
            "about",
            "Poetry",
            "emailmessage_0",
        ],
        [
            "emailmessage_2",
            "Re: Poetry",
            None,
            "user_1234",
            "about",
            "Re: Poetry",
            "emailmessage_1",
        ],
        [
            "emailmessage_3",
            "Re: Poetry",
            None,
            "user_1234",
            "about",
            "Re: Poetry",
            "emailmessage_2",
        ],
        [
            "emailmessage_4",
            "Re: Poetry",
            None,
            "user_1234",
            "about",
            "Re: Poetry",
            "emailmessage_3",
        ],
        [
            "emailmessage_5",
            "Re: Poetry",
            None,
            "user_1234",
            "about",
            "Re: Poetry",
            "emailmessage_4",
        ],
        [
            "emailmessage_1",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "emailmessage_2",
        ],
        [
            "emailmessage_2",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "emailmessage_3",
        ],
        [
            "emailmessage_3",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "emailmessage_4",
        ],
        [
            "emailmessage_4",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "emailmessage_5",
        ],
        [
            "emailmessage_0",
            "Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "emailmessage_1",
        ],
        ["false_1", "Re: Poetry", None, "user_1234", "about", "Poetry", "false_0"],
        ["false_2", "Re: Poetry", None, "user_1234", "about", "Re: Poetry", "false_1"],
        ["false_3", "Re: Poetry", None, "user_1234", "about", "Re: Poetry", "false_2"],
        ["false_4", "Re: Poetry", None, "user_1234", "about", "Re: Poetry", "false_3"],
        ["false_5", "Re: Poetry", None, "user_1234", "about", "Re: Poetry", "false_4"],
        [
            "false_1",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "false_2",
        ],
        [
            "false_2",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "false_3",
        ],
        [
            "false_3",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "false_4",
        ],
        [
            "false_4",
            "Re: Poetry",
            None,
            "user_1234",
            "subjectOf",
            "Re: Poetry",
            "false_5",
        ],
        ["false_0", "Poetry", None, "user_1234", "subjectOf", "Re: Poetry", "false_1"],
    ]
    insert_response = nexus_db.insert(relation_name, fields, values)
    print("Insert data response:", insert_response)

    # Test parameters for recursive query
    relation_name = "Graph"
    source_field = "sourceId"
    target_field = "targetId"
    starting_condition = "targetId = 'emailmessage_3'"

    # Execute the recursive query
    try:
        result = nexus_db.recursive_query(
            relation_name,
            source_field,
            target_field,
            starting_condition,
            tabulate=True,
            include_types=True,
        )
        print("Recursive Query Result:")
        print(result)
    except Exception as e:
        print(f"Error executing recursive query: {e}")


if __name__ == "__main__":
    main()
