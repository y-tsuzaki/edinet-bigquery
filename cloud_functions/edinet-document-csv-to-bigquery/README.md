# NOTE

GCSの指定ディレクトリにある EDINET　csvファイルを加工してBigQueryに投下する

およそ4000社で 2GB強のメモリを使うので注意。 実行時のメモリを大きくしてください。
どこでメモリリークしているのかわからない。


# DEPLOY COMMAND
gcloud functions deploy edinet-document-csv-to-bigquery     --gen2     --runtime python312     --trigger-http     --entry-point main     --memory 1GB     --project y-tsuzaki-sandbox   --timeout 600s --max-instances 10   --region asia-northeast1

# TEST COMMAND
 curl -m 610 -X POST https://asia-northeast1-y-tsuzaki-sandbox.cloudfunctions.net/edinet-document-csv-to-bigquery \
> -H "Authorization: bearer $(gcloud auth print-identity-token)" \
> -H "Content-Type: application/json" 

curl -m 610 -X POST https://asia-northeast1-y-tsuzaki-sandbox.cloudfunctions.net/edinet-document-csv-to-bigquery \
-H "Authorization: bearer $(gcloud auth print-identity-token)"