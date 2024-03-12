import requests
import os
from tabulate import tabulate


class NexusDB:
    def __init__(self, api_key=None):
        self.base_url = os.environ.get("BASE_URL", "https://api.nexusdb.io/query")
        self.api_key = (
            api_key if api_key is not None else os.environ.get("NEXUSDB_API_KEY")
        )

        self.headers = {"Content-Type": "application/json", "API-Key": self.api_key}

    def _process_response(self, response, tabulate_option=False):
        if not response.text:
            print("Error: Empty response from server")
            return

        try:
            response_data = response.json()
            if (
                tabulate_option
                and "headers" in response_data
                and "rows" in response_data
            ):
                # Simplify and prepare rows for tabulation
                simplified_rows = []
                for row in response_data["rows"]:
                    simplified_row = []
                    for cell in row:
                        # Simplify data structure for Num and Str types, or fallback to the original structure
                        if isinstance(cell, dict):
                            if "Num" in cell and "Int" in cell["Num"]:
                                simplified_row.append(cell["Num"]["Int"])
                            elif "Str" in cell:
                                simplified_row.append(cell["Str"])
                            else:
                                simplified_value = next(
                                    iter(cell.values()), str(cell)
                                )  # Attempt to simplify further or convert to string
                                if isinstance(simplified_value, dict):
                                    simplified_row.append(
                                        next(
                                            iter(simplified_value.values()),
                                            str(simplified_value),
                                        )
                                    )
                                else:
                                    simplified_row.append(simplified_value)
                        else:
                            simplified_row.append(cell)
                    simplified_rows.append(simplified_row)

                return tabulate(simplified_rows, headers=response_data["headers"])
            else:
                return response.text
        except json.JSONDecodeError:
            print(f"Error: Response: {response.text} could not be decoded as JSON")
            return response.text

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

    def lookup(self, relation_name, fields=None, condition="", tabulate=False):
        if fields is None:
            fields = []

        data = {
            "query_type": "Lookup",
            "relation_name": relation_name,
            "fields": fields,
            "condition": condition,
        }
        response = requests.post(self.base_url, headers=self.headers, json=data)
        return self._process_response(response, tabulate)

    def join(self, join_type, relations, return_fields, option=None, tabulate=False):
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
        return self._process_response(response, tabulate)

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

    def edit_fields(
        self,
        relation_name,
        fields=None,
        add_columns=None,
        condition="",
        access_keys=None,
    ):
        """
        Edits columns in the specified relation, optionally adding new columns.

        :param relation_name: The name of the relation to edit.
        :param fields: Optional list of fields to include in the edit.
        :param add_columns: Optional structure defining new columns to add.
        :param condition: Optional condition to apply to the edit.
        :param access_keys: Optional list of access keys for authorization.
        :return: The server's response.
        """
        # Prepare the data payload for the request
        data = {
            "query_type": "ColumnEditor",
            "relation_name": relation_name,
            "fields": fields if fields is not None else [],
            "add_columns": add_columns,
            "condition": condition,
            "access_keys": access_keys if access_keys is not None else [],
        }

        # Send the request to the server
        response = requests.post(self.base_url, headers=self.headers, json=data)

        # Return the server's response
        return response.text

    def insert_with_vector(
        self,
        relation_name,
        text,
        vectors,
        access_keys=None,
        metadata=None,
        references=None,
    ):
        payload = {
            "query_type": "Insert",
            "relation_name": relation_name,
            "searchable_content": {
                "text": text,
                "vectors": vectors,
            },
        }
        # Only add optional parameters to the payload if they are not None
        if access_keys is not None:
            payload["searchable_content"]["access_keys"] = access_keys
        if metadata is not None:
            payload["searchable_content"]["metadata"] = metadata
        if references is not None:
            payload["searchable_content"]["reference"] = references

        response = requests.post(self.base_url, json=payload, headers=self.headers)
        return response.text

    def vector_search(
        self,
        query_vector,
        access_keys=None,
        search_radius=None,
        number_of_results=None,
        filter_statement=None,
        tabulate=False,
    ):
        query_payload = {
            "query_type": "VectorSearch",
            "query_vector": query_vector,
        }

        if access_keys is not None:
            query_payload["access_keys"] = access_keys
        if search_radius is not None:
            query_payload["search_radius"] = search_radius
        if number_of_results is not None:
            query_payload["number_of_results"] = number_of_results
        if filter_statement is not None:
            query_payload["filter_statement"] = filter_statement

        response = requests.post(
            self.base_url, json=query_payload, headers=self.headers
        )
        return self._process_response(response, tabulate)
