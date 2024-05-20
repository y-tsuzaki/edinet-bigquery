# EDINET-BigQuery

このプロジェクトはGCPのサービスを用いてBigQueryにEDINETのデータを蓄積するためのものです。

# 全体の構成

- EDINET API v2を用いて提出日ごとの文書のメタ情報を取得する （Workflows)
  - ipunt: 日付のリスト
  - output: GCSにjsonファイルとして保存する
- メタ情報jsonファイル群の情報をBigQueryに投入する
  - input: GCS上のメタ情報jsonファイル群
  - output: BigQueryテーブル（edinet/documents_meta) 
- メタ情報から取得したいdocIDのみ抽出し、EDINET APIを用いてCSV（実際はTSV形式）ファイル群のZIPファイルをダウロードする（Workflows, Functions)
  - input: 日付のリスト
  - output: GCSにzipファイルとして保存する
- ZIPファイルを解凍する（Functions）
  - input: GCSイベント
  - output: zip内のcsvファイル（複数、tsv形式）
- EDINET CSVファイルをBigQueryに投入する
  - input: メタ情報テーブル（BigQuery staging_edinet/stg_documents_meta), GCS上のcsvファイル群
  - output: Bigqueryテーブル(edinet/documents)


# MEMO

### 対象
- 2023-04-01 to 2024-05-20
- secIdあり
- docTypeCode 120 (有価証券報告書)

3953社、4038ファイル 

### 抽出結果

3946社, 3946ファイル

投入漏れのリスト
```
date	docId	filerName	docDescription
2023-08-31	S100RQI5	オムニ・プラス・システム・リミテッド	有価証券報告書－第21期(2022/04/01－2023/03/31)
2023-12-25	S100SGNB	ワイ・ティー・エル・コーポレーション・バーハッド	有価証券報告書
2024-03-15	S100T22C	テックポイント・インク（Ｔｅｃｈｐｏｉｎｔ，Ｉｎｃ．）	有価証券報告書－第12期(2023/01/01－2023/12/31)
2023-04-12	S100QK8D	テックポイント・インク（Ｔｅｃｈｐｏｉｎｔ，Ｉｎｃ．）	有価証券報告書－第11期(2022/01/01－2022/12/31)
2023-06-09	S100QWLM	メディシノバ・インク	有価証券報告書
2023-05-17	S100QMRB	ＹＣＰホールディングス（グローバル）リミテッド	有価証券報告書－第2期(2022/01/01－2022/12/31)
```
