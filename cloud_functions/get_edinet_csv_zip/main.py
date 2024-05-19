import os
import json
import requests
from datetime import datetime
from google.cloud import storage
import functions_framework

@functions_framework.http
def process_edinet_data(request):

    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    data = request.get_json()

    # 環境変数から必要な情報を取得
    project = "y-tsuzaki-sandbox"
    bucket_name = "edinet-y-tsuzaki-sandbox"
    api_key = os.environ.get("EDINET_API_KEY")  or data.get('edinet_api_key')
    
    print(f"project: {project}")
    print(f"bucket_name: {bucket_name}")
    print(f"api_key: {api_key}")
    
    # 日付の取得とフォーマット
    date = data.get('date') if data and 'date' in data else datetime.now().strftime('%Y-%m-%d')
    print(f"date: {date}")

    # API URLの構築
    api_url = f"https://api.edinet-fsa.go.jp/api/v2/documents.json?date={date}&type=2&Subscription-Key={api_key}"
    print(api_url)
    response = requests.get(api_url)
    print(response)

    
    # レスポンスデータをCloud Storageに保存
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"doc_json/{date}/edinet_data_{datetime.now().isoformat()}.json")
    blob.upload_from_string(response.text, content_type='application/json')

    # 有価証券報告書のdocIDを抽出
    extracted_doc_ids = []
    if response.status_code == 200:
        results = json.loads(response.text).get('results', [])
        for item in results:
            if item.get('docTypeCode') == '120' and item.get('secCode') is not None :  # 有価証券報告書　かつ証券番号あり
                doc_id = item['docID']
                extracted_doc_ids.append(doc_id)

    # 各docIDに対してZIPファイルをダウンロードして保存
    for doc_id in extracted_doc_ids:
        zip_blob = bucket.blob(f"csv_zip/{date}/{doc_id}.zip")
        if zip_blob.exists():
            print(f"File for docId: {doc_id} already exists. Skipping download.")
            continue
        
        print(f"download start. docId: {doc_id}")
        zip_url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}?type=5&Subscription-Key={api_key}"
        zip_response = requests.get(zip_url)
        if zip_response.status_code == 200:
            zip_blob.upload_from_string(zip_response.content, content_type='application/zip')

    return {'extracted_doc_ids': extracted_doc_ids}, 200