import functions_framework
import os
import zipfile
from google.cloud import storage


@functions_framework.cloud_event
def unzip_file(cloud_event):
    data = cloud_event.data
    print(f"Received data: {data}")
        
    bucket_name = data['bucket']
    zip_blob_name = data['name']

    if not zip_blob_name.startswith('csv_zip/'):
        print(f"The file {zip_blob_name} is not in the 'csv_zip' directory. Skipping.")
        return
    if not zip_blob_name.endswith('.zip'):
        print(f"The file {zip_blob_name} is not a zip file. Skipping.")
        return

    # パスを分解
    base_path, zip_file_name = os.path.split(zip_blob_name)
    date_folder = os.path.basename(base_path)
    output_dir = f"csv_unzipped/{date_folder}/{zip_file_name.replace('.zip', '')}/"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # ZIPファイルのダウンロード
    zip_blob = bucket.blob(zip_blob_name)
    zip_file_local_path = f"/tmp/{zip_file_name}"
    zip_blob.download_to_filename(zip_file_local_path)
    print(f"Downloaded {zip_blob_name} to {zip_file_local_path}")

    # ZIPファイルの解凍
    with zipfile.ZipFile(zip_file_local_path, 'r') as zip_ref:
        zip_ref.extractall(f"/tmp/{zip_file_name.replace('.zip', '')}")
    print(f"Extracted {zip_file_name} to /tmp/{zip_file_name.replace('.zip', '')}")

    # 解凍したファイルをアップロード
    for root, _, files in os.walk(f"/tmp/{zip_file_name.replace('.zip', '')}"):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(local_file_path, f"/tmp/{zip_file_name.replace('.zip', '')}")
            blob_name = os.path.join(output_dir, relative_path)
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file_path} to {blob_name}")

    # ローカルのZIPファイルと解凍したファイルを削除
    os.remove(zip_file_local_path)
    for root, _, files in os.walk(f"/tmp/{zip_file_name.replace('.zip', '')}"):
        for file_name in files:
            os.remove(os.path.join(root, file_name))
    print(f"Cleaned up local files.")

