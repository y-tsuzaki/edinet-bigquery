import os
import zipfile
import re

from google.cloud import storage

def unzip_file(request):
    storage_client = storage.Client()
    bucket_name = 'edinet-y-tsuzaki-sandbox'
    bucket = storage_client.bucket(bucket_name)
    source_directory = 'csv_zip'
    destination_directory = 'csv_unzipped'

    #　すでにunzip済みのdocIdを抽出する
    already_unziped_docIds = []
    dist_blobs = bucket.list_blobs(prefix=destination_directory)
    for blob in dist_blobs:
        PATTERN = r'/(\w+)/XBRL_TO_CSV'
        match = re.search(PATTERN, blob.name)

        if match:
            docId = match.group(1)
            already_unziped_docIds.append(docId)

    print(f"already_unziped_docIds: {already_unziped_docIds}")

    # unzip
    blobs = bucket.list_blobs(prefix=source_directory)
    for blob in blobs:
        try:
            zip_blob_name = blob.name
            if not zip_blob_name.endswith('.zip'):
                print(f"The file {zip_blob_name} is not a zip file. Skipping.")
                continue

            # パスを分解
            base_path, zip_file_name = os.path.split(zip_blob_name)
            date_folder = os.path.basename(base_path)
            output_dir = f"{destination_directory}/{date_folder}/{zip_file_name.replace('.zip', '')}/"

            # 出力ディレクトリに既にファイルが存在するかチェック
            if zip_file_name in already_unziped_docIds :
                print(f"files already exist in {output_dir}. Skipping. docId:{zip_file_name}")
                continue

            # ZIPファイルのダウンロード
            zip_file_local_path = f"/tmp/{zip_file_name}"
            blob.download_to_filename(zip_file_local_path)
            print(f"Downloaded {zip_blob_name} to {zip_file_local_path}")

            # ZIPファイルの解凍
            try:
                with zipfile.ZipFile(zip_file_local_path, 'r') as zip_ref:
                    zip_ref.extractall(f"/tmp/{zip_file_name.replace('.zip', '')}")
                print(f"Extracted {zip_file_name} to /tmp/{zip_file_name.replace('.zip', '')}")
            except zipfile.BadZipFile:
                print(f"Failed to unzip {zip_blob_name}. Skipping.")
                continue

            # 解凍したファイルをアップロード
            for root, _, files in os.walk(f"/tmp/{zip_file_name.replace('.zip', '')}"):
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_file_path, f"/tmp/{zip_file_name.replace('.zip', '')}")
                    blob_name = os.path.join(output_dir, relative_path)
                    new_blob = bucket.blob(blob_name)
                    new_blob.upload_from_filename(local_file_path)
                    print(f"Uploaded {local_file_path} to {blob_name}")

            # ローカルのZIPファイルと解凍したファイルを削除
            os.remove(zip_file_local_path)
            for root, _, files in os.walk(f"/tmp/{zip_file_name.replace('.zip', '')}"):
                for file_name in files:
                    os.remove(os.path.join(root, file_name))
            print(f"Cleaned up local files.")

        except Exception as e:
            print(f"Error processing file {zip_blob_name}: {e}")

    return "Processing complete.", 200