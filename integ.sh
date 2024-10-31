#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <OpenSearch URL> <Mapping File> <Data File>"
    exit 1
fi

# Assign the command-line arguments to variables
OPENSEARCH_URL=$1
MAPPING_FILE=$2
DATA_FILE=$3

# Index name (you can change this to any name you like)
INDEX_NAME="my_index"

# Step 1: Create OpenSearch index with specified mapping
echo "Creating index '$INDEX_NAME' with mapping from '$MAPPING_FILE'..."
curl -X PUT "$OPENSEARCH_URL/$INDEX_NAME" -H 'Content-Type: application/json' -d @"$MAPPING_FILE"

# Step 2: Bulk index the data from the file
echo "Indexing data from '$DATA_FILE' into '$INDEX_NAME'..."
curl -X POST "$OPENSEARCH_URL/$INDEX_NAME/_bulk" -H 'Content-Type: application/json' --data-binary @"$DATA_FILE"

echo "Data indexing complete."