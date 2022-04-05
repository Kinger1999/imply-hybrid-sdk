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
GS_BASE_DIR = "gs://mic_songstats_imply_parquet/mic.SongStats.Imply.Parquet"

VIRTUAL_COL_DATES = {
    "2021-12-31": "20220331T072419.237340-ca3bb661aff2",
    "2022-01-01": "20220330T131048.021182-6cb85a2f6e50",
    "2022-01-02": "20220330T132509.899198-6b245eae5774",
    "2022-01-03": "20220330T205343.126587-14d9f64d9694",
    "2022-01-04": "20220330T135710.706112-f6616c0ae201",
    "2022-01-05": "20220330T205710.741918-ed5d702c1cef",
    "2022-01-06": "20220330T165247.778606-fc04747bbc43",
    "2022-01-07": "20220330T165300.122461-745adc17e248",
    "2022-01-08": "20220330T171829.282323-fad14e51c22b",
    "2022-01-09": "20220330T205714.440131-f289d1ab5dd7",
    "2022-01-10": "20220330T175825.077991-610180227963",
    "2022-01-11": "20220330T205709.980489-ae53f9265c11",
    "2022-01-12": "20220330T181009.718348-8e155f1eb936",
    "2022-01-13": "20220330T182919.857853-18eaaa08ddd2",
    "2022-01-14": "20220330T183818.087070-9b5a786a66f0",
    "2022-01-15": "20220330T191019.800042-f6bea6fc68a2",
    "2022-01-16": "20220330T191021.178578-35cbc0ab0ef1",
    "2022-01-17": "20220330T200708.329398-9bf778915d95",
    "2022-01-18": "20220330T200529.073253-ab9b97e9398c",
    "2022-01-19": "20220330T205240.262284-ccde9f173101",
    "2022-01-20": "20220330T205405.698273-31bccaef880d",
    "2022-01-21": "20220330T205414.640481-dc8dbbfec017",
    "2022-01-22": "20220330T205411.787056-a8160a945566",
    "2022-01-23": "20220330T205407.243556-e0c8b64a2b21",
    "2022-01-24": "20220330T205428.801695-09fd1e6e9e66",
    "2022-01-25": "20220330T213213.475793-a5e0c327210c",
    "2022-01-26": "20220330T213845.664149-c1a2ccde7b10",
    "2022-01-27": "20220330T221149.326994-25c65d16153e",
    "2022-01-28": "20220330T224006.110106-85f87fe84cc2",
    "2022-01-29": "20220330T225227.509636-7fa2e7efbf8f",
    "2022-01-30": "20220330T225236.906232-e903db6e4202",
    "2022-01-31": "20220330T230307.946334-1263243d7952",
    "2022-02-01": "20220330T231133.159320-83ee5481da6b",
    "2022-02-02": "20220330T232118.861454-c1fdfd7d50f5",
    "2022-02-03": "20220330T232726.427508-2dc486058889",
    "2022-02-04": "20220330T234307.585756-9db69d0468fd",
    "2022-02-05": "20220330T235022.800801-b6ce17490468",
    "2022-02-06": "20220330T235600.657825-e5ba020f1a6b",
    "2022-02-07": "20220330T235607.722660-edcc871acd94",
    "2022-02-08": "20220331T003309.082454-865435169db7",
    "2022-02-09": "20220331T003337.644296-5eaff2df9fca",
    "2022-02-10": "20220331T004145.444728-480b4dea540d",
    "2022-02-11": "20220331T011449.045637-9701955d5c34",
    "2022-02-12": "20220331T011457.429889-1df5ddee05d4",
    "2022-02-13": "20220331T014733.400396-0c4f0f0e7ffd",
    "2022-02-14": "20220331T014728.512580-f117d3386b1a",
    "2022-02-15": "20220331T021901.494730-4bdbead4dc72",
    "2022-02-16": "20220331T022610.800839-6ba79857d3d1",
    "2022-02-17": "20220331T024526.495383-645baf0c6967",
    "2022-02-18": "20220331T025120.786500-ef12ba9f4fa9",
    "2022-02-19": "20220331T025124.755402-ff3ebe0159cc",
    "2022-02-20": "20220331T031931.832757-16edf9fc0c17",
    "2022-02-21": "20220331T033739.955699-ba237ed54f27",
    "2022-02-22": "20220331T035408.788447-25b61886f149",
    "2022-02-23": "20220331T040831.126927-c9354f999173",
    "2022-02-24": "20220330T163038.814320-ea81c12e8e78",
    "2022-02-25": "20220330T210459.379796-69a4d3213fd2",
    "2022-02-26": "20220330T211030.854750-c316cae1aa40",
    "2022-02-27": "20220330T171010.802637-b425818ab0b5",
    "2022-02-28": "20220330T171913.878850-a22cf7e4069f",
    "2022-03-01": "20220330T180003.805038-317b450004a1",
    "2022-03-02": "20220330T180844.062337-69e65a3574fe",
    "2022-03-03": "20220330T184334.155201-21fee6c466aa",
    "2022-03-04": "20220330T190824.963717-1927c06352e3",
    "2022-03-05": "20220330T192034.331270-225e6f7ae7be",
    "2022-03-06": "20220330T194031.797957-0e3cf82d22b9",
    "2022-03-07": "20220330T195012.260777-f5dcb7e4732e",
    "2022-03-08": "20220330T210538.177542-b642f5ad939d",
    "2022-03-09": "20220330T203402.883342-c95a378ea6d8",
    "2022-03-10": "20220330T210914.892260-abe82f844805",
    "2022-03-11": "20220330T214302.044100-6dc88a95558f",
    "2022-03-12": "20220330T214901.013484-7044cdbbfd27",
    "2022-03-13": "20220330T215031.402144-7257dffde2ea",
    "2022-03-14": "20220330T222643.967933-1643747d87ff",
    "2022-03-15": "20220330T222835.738540-a2a8857750ff",
    "2022-03-16": "20220330T225051.700676-f05e0478201d",
    "2022-03-17": "20220330T225355.116531-0ee15d8960e4",
    "2022-03-18": "20220330T232308.968663-fa179645cf24",
    "2022-03-19": "20220330T233252.728678-0fbf2795bce3",
    "2022-03-20": "20220330T235909.037087-217f767e84a1",
    "2022-03-21": "20220331T011104.107385-577f09d7ce3a",
    "2022-03-22": "20220331T014848.855311-d36c14101100",
    "2022-03-23": "20220331T022123.737695-66226ec6428b",
    "2022-03-24": "20220331T022138.108412-57d0ba2a4f49",
    "2022-03-25": "20220331T025210.904742-ac2698ca420d",
    "2022-03-26": "20220331T025551.408667-ca2b945cdac7",
    "2022-03-27": "20220331T034512.719725-fa9579fc45e7",
    "2022-03-28": "20220331T034721.349382-f1292eb3aae1"
}


for day in VIRTUAL_COL_DATES:

    DESTINATION_TABLE = "charts_jk_test"

    ext = VIRTUAL_COL_DATES[day]

    prefix = f"{GS_BASE_DIR}/{day}/{ext}/"

    print(prefix)

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
    INGEST_SQL = INGEST_SQL.replace("{ext}", ext)
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








