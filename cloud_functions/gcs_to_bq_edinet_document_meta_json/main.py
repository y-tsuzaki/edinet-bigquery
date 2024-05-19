import functions_framework
from google.cloud import storage, bigquery
import csv
import json
import os
from datetime import datetime
import logging
import re

# ロギングの設定
logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'y-tsuzaki-sandbox'
GCS_BUCKET_NAME = 'edinet-y-tsuzaki-sandbox'
GCS_JSON_PATH = 'doc_json'
GCS_OUTPUT_PATH = 'temp_for_bq/documents_meta.csv'
BQ_DATASET = 'edinet'
BQ_TABLE = 'documents_meta'
BATCH_SIZE = 10

@functions_framework.http
def main(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    # JSONファイルのリストを取得
    blobs = list_blobs_with_prefix(GCS_BUCKET_NAME, GCS_JSON_PATH)

    all_data = []
    is_first_batch = True  # 初回バッチのフラグ

    for blob in blobs:
        if blob.name.endswith('.json'):
            logging.info(f'start filename: {blob.name}')

            local_json_path = f'/tmp/{blob.name.split("/")[-1]}'
            download_blob(GCS_BUCKET_NAME, blob.name, local_json_path)

            logging.info(f'download filename: {blob.name}')
            
            with open(local_json_path, 'r', encoding='utf-8') as f:
                file_content = f.read()

                print(f'loaded :{file_content}')

                match = re.search(r'/(\d{4}-\d{2}-\d{2})/', blob.name)
                if match:
                    date_str = match.group(1)
                    print(date_str)  # 出力: 2023-04-10
                else:
                    print("日付が見つかりませんでした")
                    raise Exception("日付が見つかりませんでした")
                
                all_data.append({
                    "date": date_str,
                    "json": file_content
                })

            # バッチサイズに達したらBigQueryに書き込み
            if len(all_data) >= BATCH_SIZE:
                load_data_to_bigquery(BQ_DATASET, BQ_TABLE, all_data, is_first_batch)
                is_first_batch = False  # 初回バッチが完了したためフラグを更新
                all_data.clear()

    # 残りのデータを書き込み
    if all_data:
        load_data_to_bigquery(BQ_DATASET, BQ_TABLE, all_data, is_first_batch)

    return 'done'

def list_blobs_with_prefix(bucket_name, prefix):
    """指定されたGCSバケット内のプレフィックスで始まるファイルのリストを取得"""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return blobs

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """GCSからファイルをダウンロード"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

def load_data_to_bigquery(dataset_id, table_id, rows_to_insert, is_first_batch):
    """データをBigQueryテーブルにロード"""
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if is_first_batch else bigquery.WriteDisposition.WRITE_APPEND

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("json", "JSON")
        ],
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
    job.result()  # ジョブの完了を待機

    if job.errors:
        print(f"Errors occurred while loading rows: {job.errors}")
