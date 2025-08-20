import subprocess
import os

def upload_generated_data_to_hdfs(local_csv_path, hdfs_target_path, docker_container_name="hadoop-namenode"):

    try:
        print("Copying file into the Hadoop Docker container...")
        subprocess.run([
            "docker", "cp", local_csv_path,
            f"{docker_container_name}:/tmp/data.csv"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(" Failed to copy file into the container:", e)
        return

    try:
        print("Uploading file to HDFS...")
        subprocess.run([
            "docker", "exec", docker_container_name,
            "hdfs", "dfs", "-put", "-f", "/tmp/data.csv", hdfs_target_path
        ], check=True)
        print(f"✅ File successfully uploaded to HDFS at: {hdfs_target_path}")
    except subprocess.CalledProcessError as e:
        print("Failed to upload file to HDFS:", e)



