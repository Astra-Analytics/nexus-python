from nexus_python.nexusdb import NexusDB
from dotenv import load_dotenv
import os
from openai import OpenAI
import requests

# Load environment variables from .env file
load_dotenv()


def get_embedding(text):
    client = OpenAI()
    response = client.embeddings.create(input=text, model="text-embedding-ada-002")
    return response.data[0].embedding


def main():
    # Initialize NexusDB
    nexus_db = NexusDB()

    # Example text to insert
    text_to_insert = "nexusdb is a great database."

    # First, get the vector for the given text
    vector = get_embedding(text_to_insert)

    insert_response = nexus_db.insert_with_vector(
        relation_name="relation_name_of_reference",
        text=text_to_insert,
        vectors=vector,
    )
    print("Insert Response:", insert_response)

    # Search for similar vectors
    search_query = "Similar search query to find relevant texts."
    query_vector = get_embedding(search_query)

    # Perform the search using the vector
    # This assumes the vector_search in NexusDB can handle raw query vectors
    search_response = nexus_db.vector_search(
        query_vector=query_vector,  # Adjust parameters as needed
        number_of_results=5,  # Example parameter
    )
    print("Search Response:", search_response)


if __name__ == "__main__":
    main()
