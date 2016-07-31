require 'googleauth'
require 'googleauth/stores/file_token_store'
require 'google/apis/bigquery_v2'

class BigQueryConnection
  GOOGLE_CRED_JSON_PATH      = File.join(File.expand_path(File.dirname(__FILE__)), "./google-auth-cred.json")
  ServiceAccountCredentials  = Google::Auth::ServiceAccountCredentials
  BigQuery                   = Google::Apis::BigqueryV2
  PROJECT_ID                 = "igaiga"
  DATASET_ID                 = "igaiga0725"

  def initialize
    @bigquery = BigQuery::BigqueryService.new
    @bigquery.authorization = ServiceAccountCredentials.make_creds(
      json_key_io: File.open(GOOGLE_CRED_JSON_PATH),
      scope: [
        BigQuery::AUTH_BIGQUERY,
        BigQuery::AUTH_CLOUD_PLATFORM,
        BigQuery::AUTH_DEVSTORAGE_FULL_CONTROL,
        BigQuery::AUTH_BIGQUERY_INSERTDATA
      ]
    )

    @bigquery
  end

  # テーブル情報取得
  def table(table_name)
    @bigquery.get_table(PROJECT_ID, DATASET_ID, table_name)
  end
end

bqc = BigQueryConnection.new
p bqc.table("tests")
