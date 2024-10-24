import csv
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Connect to Elasticsearch
# es = Elasticsearch(
#     ['http://localhost:9200'],
#     basic_auth=('elastic', 'changeme')
# )

es = Elasticsearch(
    ['http://192.168.200.161:9200'],
    basic_auth=('elastic', 'hust@2024')
)

# Define the index name and mapping
index_name = 'amazon_stock'
mapping = {
    'mappings': {
        'properties': {
            'Date': {'type': 'date', 'format': 'yyyy-MM-dd'},
            'Open': {'type': 'float'},
            'High': {'type': 'float'},
            'Low': {'type': 'float'},
            'Close': {'type': 'float'},
            'Adj Close': {'type': 'float'},
            'Volume': {'type': 'long'}
        }
    }
}

# Create the index with the mapping
es.indices.create(index=index_name, body=mapping, ignore=400)


def read_csv_and_index(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')

        actions = []
        for row in reader:
            # Convert date string to datetime object
            # date = datetime.strptime(row['Date'], '%Y-%m-%d')

            # Create document
            doc = {
                '_index': index_name,
                '_id': row['Date'],
                '_source': {
                    'Date': row['Date'],
                    'Open': float(row['Open']),
                    'High': float(row['High']),
                    'Low': float(row['Low']),
                    'Close': float(row['Close']),
                    'Adj Close': float(row['Adj Close']),
                    'Volume': int(row['Volume'])
                }
            }
            actions.append(doc)

            # If we have 100 documents, send them to Elasticsearch
            if len(actions) == 100:
                helpers.bulk(es, actions)
                actions = []

        # Send any remaining documents
        if actions:
            helpers.bulk(es, actions)


# Run the function
read_csv_and_index('AMZN.csv')

print("Data indexing complete.")