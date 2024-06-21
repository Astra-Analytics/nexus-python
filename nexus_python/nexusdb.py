import json
import logging
import os

import requests
from tabulate import tabulate

# Initialize logger
logger = logging.getLogger("nexusdb")
logger.setLevel(logging.WARNING)
logger.addHandler(logging.NullHandler())


class NexusDB:
    def __init__(self, api_key=None):
        self.base_url = os.environ.get("BASE_URL", "https://api.nexusdb.io/query")
        self.api_key = (
            api_key if api_key is not None else os.environ.get("NEXUSDB_API_KEY")
        )

        self.headers = {"Content-Type": "application/json", "API-Key": self.api_key}

    @staticmethod
    def configure_logging(level=logging.INFO, filename=None):
        """Configure logging for debugging purposes."""
        logger.setLevel(level)
        handler = (
            logging.StreamHandler()
            if filename is None
            else logging.FileHandler(filename)
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    def _process_response(self, response, tabulate_option: bool, include_types: bool):
        if not response.text:
            error = "Error: Empty response from server"
            return error

        try:
            response_data = response.json()
            if "headers" in response_data and "rows" in response_data:
                headers = response_data["headers"]
                rows = response_data["rows"]

                def extract_value_and_type(cell):
                    if isinstance(cell, dict):
                        for key, value in cell.items():
                            if key == "Num":
                                if "Int" in value:
                                    return value["Int"], "Int"
                                elif "Float" in value:
                                    return value["Float"], "Float"
                            elif key == "Str":
                                return value, "Str"
                            elif key == "Bool":
                                return value, "Bool"
                            elif key == "Uuid":
                                return value, "Uuid"
                            elif key == "Json":
                                return value, "Json"
                            elif key == "List":
                                # Recursively process nested lists
                                return [
                                    extract_value_and_type(item)[0] for item in value
                                ], "List"
                        return str(cell), "Unknown"
                    return cell, "Unknown"

                if include_types:
                    if rows:
                        # Extract types from the first row
                        first_row = rows[0]
                        typed_headers = [
                            f"{headers[i]} ({extract_value_and_type(cell)[1]})"
                            for i, cell in enumerate(first_row)
                        ]
                    else:
                        typed_headers = headers

                simplified_rows = [
                    [extract_value_and_type(cell)[0] for cell in row] for row in rows
                ]

                if tabulate_option:
                    if include_types:
                        return tabulate(simplified_rows, headers=typed_headers)
                    else:
                        return tabulate(simplified_rows, headers=headers)
                else:
                    if include_types:
                        return json.dumps(response_data)
                    else:
                        # Modify the response to exclude types
                        response_data["rows"] = simplified_rows
                        return json.dumps(response_data)
            else:
                return response.text
        except json.JSONDecodeError:
            error = f"Error: Response: {response.text} could not be decoded as JSON"
            return error

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

        logger.debug(
            f"Creating relation {relation_name} with columns: {formatted_columns}"
        )
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Create response: {response.text}")
        return response.text

    def modify_data(
        self,
        operation_type,
        relation_name,
        fields=None,
        values=None,
        text=None,
        embeddings=None,
        access_keys=None,
        metadata=None,
        references=None,
    ):
        """
        Modifies data in the specified relation, handling insert, upsert, and update operations
        with optional parameters like embeddings, metadata, etc.

        :param operation_type: The type of operation ("Insert", "Upsert", "Update").
        :param relation_name: The name of the relation to modify.
        :param fields: List of fields to include in the operation.
        :param values: List of values to include in the operation.
        :param text: Optional text content for vector-based operations.
        :param embeddings: Optional vector content for vector-based operations.
        :param access_keys: Optional access keys for authorization.
        :param metadata: Optional metadata for the operation.
        :param references: Optional references for the operation.
        :return: The server's response.
        """
        # Validation logic
        if (fields is None) != (values is None):
            raise ValueError("Both fields and values must be specified together.")
        if (text is None) != (embeddings is None):
            raise ValueError("Both text and embeddings must be specified together.")
        if fields is None and text is None:
            raise ValueError(
                "You must specify fields/values or text/embeddings, or both."
            )

        payload = {
            "query_type": operation_type,
            "relation_name": relation_name,
        }

        if fields is not None and values is not None:
            payload["fields"] = fields
            payload["values"] = values
        if text is not None and embeddings is not None:
            payload["searchable_content"] = {
                "text": text,
                "embeddings": embeddings,
            }
        if access_keys is not None:
            payload["access_keys"] = access_keys
        if metadata is not None:
            if "searchable_content" not in payload:
                payload["searchable_content"] = {}
            payload["searchable_content"]["metadata"] = metadata
        if references is not None:
            if "searchable_content" not in payload:
                payload["searchable_content"] = {}
            payload["searchable_content"]["reference"] = references

        logger.debug(
            f"{operation_type} into {relation_name} with payload: {json.dumps(payload, indent=2)}"
        )
        response = requests.post(self.base_url, headers=self.headers, json=payload)
        logger.debug(f"{operation_type} response: {response.text}")
        return response.text

    def insert(
        self,
        relation_name,
        fields=None,
        values=None,
        text=None,
        embeddings=None,
        access_keys=None,
        metadata=None,
        references=None,
    ):
        return self.modify_data(
            "Insert",
            relation_name,
            fields,
            values,
            text,
            embeddings,
            access_keys,
            metadata,
            references,
        )

    def upsert(
        self,
        relation_name,
        fields=None,
        values=None,
        text=None,
        embeddings=None,
        access_keys=None,
        metadata=None,
        references=None,
    ):
        return self.modify_data(
            "Upsert",
            relation_name,
            fields,
            values,
            text,
            embeddings,
            access_keys,
            metadata,
            references,
        )

    def update(
        self,
        relation_name,
        fields=None,
        values=None,
        text=None,
        embeddings=None,
        access_keys=None,
        metadata=None,
        references=None,
    ):
        return self.modify_data(
            "Update",
            relation_name,
            fields,
            values,
            text,
            embeddings,
            access_keys,
            metadata,
            references,
        )

    def lookup(
        self,
        relation_name,
        fields=None,
        condition="",
        tabulate=False,
        include_types=False,
    ):
        if fields is None:
            fields = []

        data = {
            "query_type": "Lookup",
            "relation_name": relation_name,
            "fields": fields,
            "condition": condition,
        }
        logger.debug(
            f"Looking up {relation_name} with fields: {fields} and condition: {condition}"
        )
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Lookup response: {response.text}")
        return self._process_response(response, tabulate, include_types)

    def join(
        self,
        join_type,
        relations,
        return_fields,
        option=None,
        tabulate=False,
        include_types=False,
    ):
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

        logger.debug(
            f"Executing {join_type} join on relations: {relations} with return fields: {return_fields} and option: {option}"
        )
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Join response: {response.text}")
        return self._process_response(response, tabulate, include_types)

    def delete(self, relation_name, condition):
        """Deletes data from the specified relation where condition is met."""

        data = {
            "query_type": "Delete",
            "relation_name": relation_name,
            "condition": condition,
        }
        logger.debug(f"Deleting from {relation_name} where condition: {condition}")
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Delete response: {response.text}")
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

        logger.debug(
            f"Editing fields for {relation_name} with fields: {fields}, add_columns: {add_columns}, condition: {condition}"
        )
        # Send the request to the server
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Edit fields response: {response.text}")
        # Return the server's response
        return response.text

    def vector_search(
        self,
        query_vector,
        access_keys=None,
        search_radius=None,
        number_of_results=None,
        filter_statement=None,
        tabulate=False,
        include_types=False,
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

        logger.debug(
            f"Performing vector search with access keys: {access_keys}, search radius: {search_radius}, number of results: {number_of_results}, filter statement: {filter_statement}"
        )
        response = requests.post(
            self.base_url, json=query_payload, headers=self.headers
        )
        logger.debug(f"Vector search response: {response.text}")
        return self._process_response(response, tabulate, include_types)

    def recursive_query(
        self,
        relation_name,
        source_field,
        target_field,
        starting_condition,
        tabulate=False,
        include_types=False,
    ):
        """
        Executes a recursive query with the specified parameters.

        :param relation_name: The name of the relation to query.
        :param source_field: The source field for the recursion.
        :param target_field: The target field for the recursion.
        :param starting_condition: The starting condition for the recursion.
        :param return_fields: Fields to be returned in the result.
        :return: The result of the recursive query as a JSON object.
        """
        relation = {
            "relation_name": relation_name,
            "fields": [],  # Will be populated by the server
            "condition": starting_condition,
            "defaults": None,
            "access_keys": None,
        }
        data = {
            "query_type": "Recursion",
            "relation": relation,
            "source": source_field,
            "target": target_field,
        }

        logger.debug(
            f"Executing recursive query on relation: {relation_name} with source field: {source_field}, "
            f"target field: {target_field}, starting condition: {starting_condition}"
        )
        response = requests.post(self.base_url, headers=self.headers, json=data)
        logger.debug(f"Recursive query response: {response.text}")
        return self._process_response(response, tabulate, include_types)
