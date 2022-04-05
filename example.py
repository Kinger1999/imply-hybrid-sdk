from imply_hybrid_sdk import ImplyPrivateAuthenticator, ImplyTaskApi, ImplyClusterQuery, SQLQuery
import time
import sys


auth = ImplyPrivateAuthenticator(
    endpoint="http://localhost:8888",
    username="admin",
    password="E8riBZXQmk+BaQlg9LeaAA==",
)
response = auth.test()
print(response.status_code, response.text)


#
# Talaria Query Server
#
GS_BASE_DIR = "gs://mic_songstats_imply_parquet/mic.SongStats.Imply.Parquet/"
VIRTUAL_COL_DATES = [
    "2022-01-01"
]


for day in VIRTUAL_COL_DATES:

    DESTINATION_TABLE = "charts_jk_test"

    prefix = f"{GS_BASE_DIR}/{day}/20220330T131048.021182-6cb85a2f6e50/"

    INGEST_SQL = """
    INSERT INTO {DESTINATION_TABLE}
    SELECT
      TIME_PARSE("date") AS __time,
      track_id, 
      country_code, 
      region_code, 
      city, 
      gender, 
      ageBucket, 
      source,
       is_premium, 
       count(*) as streams, 
       APPROX_COUNT_DISTINCT_DS_HLL(user_id,14,'HLL_4') as listeners
    FROM TABLE(
      EXTERN(
        '{"type":"google","prefixes":["{prefix}"]}',
        '{"type": "parquet"}',
        '[
            {"name": "date", "type": "string"}, 
            {"name": "track_id", "type": "string"}, 
            {"name": "track_id_hash", "type": "string"}, 
            {"name": "country_code", "type": "string"}, 
            {"name": "city", "type": "string"}, 
            {"name": "region_code", "type": "string"}, 
            {"name": "gender", "type": "string"}, 
            {"name": "ageBucket", "type": "string"}, 
            {"name": "source", "type": "string"}, 
            {"name": "artist_id", "type": "string"},
            {"name": "is_premium", "type": "string"}, 
            {"name": "user_id", "type": "string"}, 
            {"name": "user_id_hash", "type": "string"}, 
            {"name": "context_playlist_owner_type", "type": "string"}, 
            {"name": "context_playlist_top_type", "type": "string"}, 
            {"name": "context_playlist_sub_type", "type": "string"}, 
            {"name": "context_top_type", "type": "string"}, 
            {"name": "context_sub_type", "type": "string"}
        ]'
      )
    )
    WHERE FLOOR(TIME_PARSE("date") TO DAY) = TIMESTAMP '{day}'
    GROUP BY 1,2,3,4,5,6,7,8,9
    PARTITIONED BY DAY
    CLUSTERED BY track_id, country_code, region_code, city, source, gender
    """
    INGEST_SQL = INGEST_SQL.replace("{DESTINATION_TABLE}", DESTINATION_TABLE)
    INGEST_SQL = INGEST_SQL.replace("{prefix}", prefix)
    INGEST_SQL = INGEST_SQL.replace("{day}", day)
    print(INGEST_SQL)

    query = SQLQuery()
    query.set_query(INGEST_SQL)
    query.set_context({
        "talaria": True,
        "talariaFinalizeAggregations": False,
        "talariaReplaceTimeChunks": f"{day}/P1D",  # This needs to change based on dataset
        "talariaNumTasks": 10,
        "talariaRowsinMemory": 50000,
        "talariaRowsPerSegment": 25000000

    })

    query_server = ImplyClusterQuery(auth=auth)
    response = query_server.sql_query(query=query)  # returns response object with task

    if response.status_code == 200:
        task_id = response.json()[0]['TASK']

        task_api = ImplyTaskApi(auth=auth)

        # wait for the query to finish
        while True:
            time.sleep(5)
            response = task_api.task_status(task_id=task_id)
            print(response.status_code, response.json()['status']['status'], response.json()['status'])
            if response.json()['status']['status'] == "SUCCESS":
                break
            if response.json()['status']['status'] == "FAILED":
                # Do some error handling here
                break

        # all segments are published... continue

    else:
        #handle non 200 here
        print(response.status_code, response.text)








