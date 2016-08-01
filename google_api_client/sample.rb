# google-api-client
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
  end

  # テーブル情報取得
  def table(table_name)
    @bigquery.get_table(PROJECT_ID, DATASET_ID, table_name)
  end

  def create_table
    table_name = "test2"
    schema = [
        { name: "id", type: "INTEGER", mode: 'required', description: "id"},
        { name: "name", type: "STRING", description: "name"},
        { name: "count", type: "INTEGER", description: "count"}
      ]

    table = BigQuery::Table.new(
      table_reference: { project_id: PROJECT_ID, dataset_id: DATASET_ID, table_id: "test2" },
      schema: { fields: schema }
      )
    @bigquery.insert_table(PROJECT_ID, DATASET_ID, table)
  end

  def insert_all
    rows = [
        {
          :insertId => "foo1",
          :json => { name: "foo1", data: "bar" }
        },
        {
          :insertId => "foo2",
          :json => { name: "foo2", data: "baz" }
        },
      ]
    insert_all_table_data_request = Google::Apis::BigqueryV2::InsertAllTableDataRequest.new({
        :rows => rows
                                                                                              })

    p @bigquery.insert_all_table_data(PROJECT_ID, DATASET_ID, "tests", insert_all_table_data_request)
  end
end

bqc = BigQueryConnection.new
#p bqc.table("tests")
#bqc.create_table
bqc.insert_all

