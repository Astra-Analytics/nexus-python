import requests
import os


class NexusDB:
    def __init__(self, api_key=None):
        self.base_url = os.environ.get("BASE_URL", "https://api.nexusdb.io/query")
        # Use the provided API key or fallback to the environment variable
        self.api_key = (
            api_key if api_key is not None else os.environ.get("NEXUSDB_API_KEY")
        )
        self.headers = {"Content-Type": "application/json", "API-Key": self.api_key}

    def create(self, relation_name, columns):
        """Creates a new relation with the specified columns, making adjustments for optional parameters."""
        formatted_columns = []

        # Assume the first column is primary if none is specified
        primary_seen = any(
            "is_primary" in column and column["is_primary"] for column in columns
        )

        for i, column in enumerate(columns):
            # Apply defaults for missing 'type' and 'default'
            column_type = column.get("type", "Any?")
            column_default = column.get("default", None)

            # If is_primary is not specified, only the first column is assumed to be primary (if no other primary is set)
            if "is_primary" not in column:
                column_is_primary = not primary_seen and i == 0
                if column_is_primary:
                    primary_seen = True  # Mark that a primary column has been set
            else:
                column_is_primary = column["is_primary"]

            formatted_columns.append(
                {
                    "name": column["name"],
                    "type": column_type,
                    "default": column_default,
                    "is_primary": column_is_primary,
                }
            )

        data = {
            "query_type": "Create",
            "relation_name": relation_name,
            "fields": formatted_columns,
        }

        response = requests.post(self.base_url, headers=self.headers, json=data)
        return response.text

    def insert(self, relation_name, fields, values):
        """Inserts data into the specified relation."""
        data = {
            "query_type": "Insert",
            "relation_name": relation_name,
            "fields": fields,
            "values": values,
        }

        response = requests.post(self.base_url, headers=self.headers, json=data)

        return response.text

    def upsert(self, relation_name, fields, values):
        """Inserts data into the specified relation."""
        data = {
            "query_type": "Upsert",
            "relation_name": relation_name,
            "fields": fields,
            "values": values,
        }

        response = requests.post(self.base_url, headers=self.headers, json=data)

        return response.text

    def update(self, relation_name, fields, values):
        """Inserts data into the specified relation."""
        data = {
            "query_type": "Update",
            "relation_name": relation_name,
            "fields": fields,
            "values": values,
        }

        response = requests.post(self.base_url, headers=self.headers, json=data)

        return response.text

    def lookup(self, relation_name, fields=None, condition=""):
        """Looks up data from the specified relation."""
        # Using fields=None and then setting it to [] if None to avoid mutable default argument
        if fields is None:
            fields = []

        data = {
            "query_type": "Lookup",
            "relation_name": relation_name,
            "fields": fields,
            "condition": condition,
        }
        response = requests.post(self.base_url, headers=self.headers, json=data)
        return response.text

    def join(self, join_type, relations, return_fields, option=None):
        """
        Executes a join query with the specified parameters.

        :param join_type: The type of join (e.g., "Inner", "Outer").
        :param relations: A list of dictionaries, each representing a relation in the join.
                        Each dictionary should have 'relation_name', 'fields', and optionally 'defaults'.
        :param return_fields: A list of fields to return in the result.
        :param option: Additional options for the join query (e.g., a limit clause).
        :return: The result of the join query as a JSON object.
        """
        data = {
            "query_type": "Join",
            "join_type": join_type,
            "relations": relations,
            "return": {
                "fields": return_fields,
            },
        }

        if option is not None:
            data["return"]["option"] = option

        response = requests.post(self.base_url, headers=self.headers, json=data)
        return response.text

    def delete(self, relation_name, primary_keys):
        """Deletes data from the specified relation based on primary keys."""
        # Convert primary_keys dict to the expected list format
        primary_keys_list = [{"name": k, "value": v} for k, v in primary_keys.items()]

        data = {
            "query_type": "Delete",
            "relation_name": relation_name,
            "primary_keys": primary_keys_list,
        }
        print(f"data: {data}\n\n")
        response = requests.post(self.base_url, headers=self.headers, json=data)
        return response.text
