import happybase
from hdfs import InsecureClient

# HDFS configuration
hdfs_client = InsecureClient("http://localhost:9870", user="hadoop")
hdfs_path = "/ecommerce-data2/data.json"

# HBase configuration
hbase_connection = happybase.Connection('hbase-master')
table_name = "ecommerce"

# Create table if not exists
connection = happybase.Connection("hbase-master")
if table_name not in connection.tables():
    connection.create_table(
        table_name,
        {"customer": dict(), "transactions": dict()}
    )

# Load data from HDFS and write to HBase
table = connection.table(table_name)
print("Loading data from HDFS to HBase...")
with hdfs_client.read(hdfs_path) as reader:
    for line in reader:
        record = json.loads(line.decode("utf-8"))
        row_key = record["id"]  # Adjust field for unique row key
        table.put(row_key, {
            "customer:name": record["customer_name"],
            "transactions:amount": str(record["amount"])
        })
        print(f"Inserted into HBase: {record}")
