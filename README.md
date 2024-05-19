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
- 2023-04-01 to 2024-03-31
- secIdあり
- docTypeCode 120 (有価証券報告書)

3345社、3585ファイル

### 抽出結果

3325社、3325

投入漏れのリスト
```
date	docId	filerName
2023-05-24	S100QT9Y	株式会社ハイデイ日高
2023-05-24	S100QSKR	株式会社エーアイテイー
2023-05-24	S100QTGT	株式会社ナルミヤ・インターナショナル
2023-05-24	S100QT9J	株式会社ケーヨー
2023-08-31	S100RQI5	オムニ・プラス・システム・リミテッド
2023-05-24	S100QTDK	松竹株式会社
2023-05-24	S100QTE9	株式会社ＰＲ　ＴＩＭＥＳ
2023-05-24	S100QT9F	株式会社キャンドゥ
2023-05-24	S100QTEK	ＳＦＰホールディングス株式会社
2023-05-24	S100QTFR	株式会社エディア
2023-05-23	S100QSZ1	株式会社リンガーハット
2023-05-23	S100QT6A	株式会社アークス
2023-05-24	S100QT1J	株式会社トレジャー・ファクトリー
2023-05-17	S100QMRB	ＹＣＰホールディングス（グローバル）リミテッド
2023-04-12	S100QK8D	テックポイント・インク（Ｔｅｃｈｐｏｉｎｔ，Ｉｎｃ．）
2023-05-23	S100QSYI	株式会社トーヨーアサノ
2023-05-23	S100QSQF	株式会社ＮａＩＴＯ
2023-12-25	S100SGNB	ワイ・ティー・エル・コーポレーション・バーハッド
2023-06-09	S100QWLM	メディシノバ・インク
2023-05-24	S100QSY7	北雄ラッキー株式会社
```
