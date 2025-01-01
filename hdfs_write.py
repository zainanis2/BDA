# from kafka import KafkaConsumer
# import subprocess
# import os
# import uuid

# # Kafka Configuration
# consumer = KafkaConsumer(
#     "ecommerce-data",
#     bootstrap_servers=["localhost:9092"],
#     group_id="hdfs-writer-group",
#     auto_offset_reset="earliest",
#     enable_auto_commit=False
# )

# # HDFS Configuration
# hdfs_directory = "/ecommerce-data/"
# batch_size = 10000
# batch_counter = 0
# total_lines_inserted = 0

# # Function to Write Data in Batches to HDFS
# def write_to_hdfs(batch_data):
#     global batch_counter, total_lines_inserted
#     try:
#         # Write to a file in /tmp (accessible inside Docker container)
#         temp_file_path = f"/tmp/batch_{uuid.uuid4().hex}.txt"
#         with open(temp_file_path, "w") as temp_file:
#             temp_file.write("\n".join(batch_data))

#         # Define HDFS path
#         hdfs_path = f"{hdfs_directory}batch_{batch_counter}.txt"

#         # Upload the file to HDFS
#         process = subprocess.run(
#             [
#                 "docker",
#                 "exec",
#                 "-i",
#                 "hadoop-namenode",
#                 "bash",
#                 "-c",
#                 f"hdfs dfs -put {temp_file_path} {hdfs_path}"
#             ],
#             capture_output=True,
#             text=True,
#         )

#         if process.returncode == 0:
#             batch_counter += 1
#             total_lines_inserted += len(batch_data)
#             print(f"Batch {batch_counter} written. Total lines inserted: {total_lines_inserted}")
#         else:
#             print(f"Failed to write batch {batch_counter}. Error: {process.stderr}")

#         # Remove temporary file
#         os.remove(temp_file_path)

#     except Exception as e:
#         print(f"Exception during HDFS write: {e}")

# # Process Kafka Messages in Batches
# buffer = []
# try:
#     for message in consumer:
#         buffer.append(message.value.decode("utf-8"))
#         if len(buffer) >= batch_size:
#             write_to_hdfs(buffer)
#             consumer.commit()
#             buffer = []

# except KeyboardInterrupt:
#     print("Interrupted. Writing remaining messages...")
#     if buffer:
#         write_to_hdfs(buffer)
#         consumer.commit()
# finally:
#     print(f"Finished processing. Total batches: {batch_counter}, Total lines inserted: {total_lines_inserted}")
#     consumer.close()


import subprocess
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "ecommerce-data",
    bootstrap_servers=["localhost:9092"],
    group_id="hdfs-writer-group",
    auto_offset_reset="earliest",
)

hdfs_directory = "/ecommerce-data/"
batch_size = 13000
batch_counter = 0

def write_to_hdfs_via_cli(batch_data):
    global batch_counter
    try:
        file_name = f"batch_{batch_counter}.txt"
        hdfs_path = f"{hdfs_directory}{file_name}"
        process = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "hadoop-namenode",
                "bash",
                "-c",
                f"echo '{batch_data}' | hdfs dfs -put - {hdfs_path}",
            ],
            capture_output=True,
            text=True,
        )
        if process.returncode == 0:
            print(f"Successfully wrote batch {batch_counter} to HDFS.")
            batch_counter += 1
        else:
            print(f"Failed to write batch {batch_counter}. Error: {process.stderr}")
    except Exception as e:
        print(f"Exception during HDFS write: {e}")

buffer = []

try:
    for message in consumer:
        buffer.append(message.value.decode("utf-8"))
        if len(buffer) >= batch_size:
            write_to_hdfs_via_cli("\n".join(buffer))
            buffer = []  # Clear the buffer
except KeyboardInterrupt:
    print("Interrupted. Writing remaining messages...")
    if buffer:
        write_to_hdfs_via_cli("\n".join(buffer))
finally:
    consumer.close()