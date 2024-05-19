
import os
import logging
import traceback

import functions_framework
from google.cloud import storage, bigquery
import pandas as pd
import google.cloud.logging
 
 
# 標準 Logger の設定
logging.basicConfig(
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )
logger = logging.getLogger()
 
# Cloud Logging ハンドラを logger に接続
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
 

# Constants
BUCKET_NAME = "edinet-y-tsuzaki-sandbox"
TABLE_ID = "y-tsuzaki-sandbox.edinet.documents_v3"
TEMP_CSV_FILE_PATH = '/tmp/documents.csv'
DATASET = 'edinet'
TABLE = 'documents'

def load_tsv_from_gcs(bucket_name, file_path):
    logger.debug(f"load_tsv_from_gcs bucket_name: {bucket_name}, file_path:{file_path}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    tsv_data = blob.download_as_text(encoding="utf-16")
    return tsv_data

def parse_tsv_data(tsv_data):
    from io import StringIO
    data = StringIO(tsv_data)
    df = pd.read_csv(data, sep='\t', quotechar='"', doublequote=True, names=[
        "element_id", "element_name", "context_id", "relative_year", 
        "consolidated_or_individual", "period_or_instant", "unit_id", 
        "unit", "value"
    ])

    # 1行目はヘッダーなので消す
    df = df.drop(index=0).reset_index(drop=True)

    # 行数を追加する
    df['row'] = df.index + 1

    return df

def get_target_list():
    client = bigquery.Client()

    query = """
        WITH 
        -- 有価証券報告書かつ情報企業（証券番号あり）
        target_sec_report_meta AS (
        SELECT *
        FROM `y-tsuzaki-sandbox.staging_edinet.stg_documents_meta` 
        WHERE 
            docTypeCode = '120'
            AND
            secCode IS NOT NULL
        )
        SELECT 
        `date`,
        docId
        FROM target_sec_report_meta
    """

    query_job = client.query(query)
    results = query_job.result()
    
    documents = [{"date": row.date, "docId": row.docId} for row in results]
    return documents

def parse_docs_and_save_temp_csv(date, docId):
    """
    指定された日付のdocIdを元に　edinet csv (tsv)を読み取り、BQへの転送用のデータ（DF)を作成する。それをローカルの一時ファイル（CSV)に蓄積する。 
    都度BQに書き込むと書き込み回数の制限に引っかかるし、DFを全部メモリに持つとメモリが足りなくなるため、CSVに追記モードで書き込み、別の関数でCSVをBQに転送する。
    """
    

    logging.info(f"target date: {date}, docId: {docId}")
    print(f"target date: {date}, docId: {docId}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    path = f"csv_unzipped/{date}/{docId}/"

    blobs = bucket.list_blobs(prefix=path)
    
    df_list = []

    first_iteration = True
    for blob in blobs:
        file_path = blob.name
        # Check if the file has a .csv extension
        if not file_path.endswith('.csv'):
            continue
        
        logging.info(f"Processing file: {file_path}")
        print(f"Processing file: {file_path}")
        
        filename = file_path.split('/')[-1]
        
        # Load the TSV file from GCS
        tsv_data = load_tsv_from_gcs(BUCKET_NAME, file_path)
        
        # Parse the TSV data into a DataFrame
        df = parse_tsv_data(tsv_data)
        
        df['doc_id'] = docId
        df['filename'] = filename
        
        df_list.append(df)
    
    # merge df_list
    df = pd.concat(df_list, ignore_index=True)

    if df.empty:
        raise Exception("No data available to insert into BigQuery.")

    if not os.path.exists('/tmp'):
        os.makedirs('/tmp')

    # DataFrameをCSVに追記または新規作成
    if os.path.exists(TEMP_CSV_FILE_PATH):
        df.to_csv(TEMP_CSV_FILE_PATH, mode='a', header=False, index=False)
    else:
        df.to_csv(TEMP_CSV_FILE_PATH, mode='w', header=True, index=False)


    file_size_bytes = os.path.getsize(TEMP_CSV_FILE_PATH)
    file_size_kb = file_size_bytes / 1024

    logger.info(f"The size of the temp file is: {file_size_kb:.2f} KB")




def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""

    logger.info(f"upload_to_gcs start bucket_name:{bucket_name}, source_file_name:{source_file_name} destination_blob_name:{destination_blob_name}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

def load_csv_to_bigquery(uri, dataset_id, table_id):
    """Loads a CSV file from GCS to BigQuery."""

    logger.info(f"load_csv_to_bigquery start. uri:{uri}, dataset_id:{dataset_id}, table_id:{table_id}")
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # データ洗い替え
    )

    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete.

    destination_table = client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows to {dataset_id}:{table_id}")


@functions_framework.http
def main(request):
    """ main """
    targetList = get_target_list()

    count = 0
    for target in targetList:
        count+=1
        print(f"loop count :{count} of {len(targetList)}")

        try:
            parse_docs_and_save_temp_csv(target['date'], target['docId'])
        except Exception as e:
            # エラーメッセージとスタックトレースをロギング
            logging.warning("An error occurred: %s", e)
            logging.warning("Stack trace: %s", traceback.format_exc())
            logging.info(f"ERROR: date:{target['date']} docId: {target['docId']}")
            print(f"ERROR: date:{target['date']} docId: {target['docId']}")
    

    destination_blob_name = 'tmp/documents.json'  
    source_file_name = TEMP_CSV_FILE_PATH
    # Step 1: Upload the file to GCS
    upload_to_gcs(BUCKET_NAME, source_file_name, destination_blob_name)

    # Step 2: Load the file from GCS to BigQuery
    gcs_uri = f'gs://{BUCKET_NAME}/{destination_blob_name}'
    load_csv_to_bigquery(gcs_uri, DATASET, TABLE)

    return "Files processed successfully."
