from imply_hybrid_sdk import ImplyHybridAuthenticator, ImplyTaskApi, ImplyClusterQuery, SQLQuery
import time


auth = ImplyHybridAuthenticator(
    endpoint="https://imply-5a4-elbexter-1pnwv4vyhtknb-1245080805.us-east-1.elb.amazonaws.com:9088",
    username="admin",
    password="db2A6xRdtAB646B89kXX7A==",
    cert="/Users/jonking/Downloads/5a4790b5-8586-4792-8863-1ad5c0563f1e.crt"
)
response = auth.test()
print(response.status_code, response.text)

#
# Legacy Query Server
#

query = SQLQuery()
query.set_query("SELECT COUNT(1) as counter FROM \"wikipedia\"")

query_server = ImplyClusterQuery(auth=auth)
response = query_server.sql_query(query=query)
print(response.status_code, response.text)

#
# Talaria Query Server
#

datasource = "wikipedia_3"

query = SQLQuery()
query.set_query(f"INSERT INTO {datasource} SELECT * FROM \"wikipedia\" PARTITIONED BY DAY")
query.set_context({
    "talaria": True
})

query_server = ImplyClusterQuery(auth=auth)
response = query_server.sql_query(query=query)  # returns response object with task


if response.status_code == 200:
    task_id = response.json()[0]['TASK']

    task_api = ImplyTaskApi(auth=auth)

    # wait for the query to finish
    while True:
        time.sleep(1)
        response = task_api.task_status(task_id=task_id)
        print(response.status_code, response.json()['status']['status'], response.json()['status'])
        if response.json()['status']['status'] == "SUCCESS":
            break
        if response.json()['status']['status'] == "FAILED":
            # Do some error handling here
            break

    # wait for the segments to be published
    while True:
        time.sleep(1)
        query = SQLQuery()
        query.set_query(f"select count(1) FILTER (WHERE is_published = 1) as published, count(1) FILTER (WHERE is_published = 0) as unpublished from sys.segments where datasource = '{datasource}'")
        response = query_server.sql_query(query=query)
        print(response.status_code, response.text)

        if response.status_code == 200:
            if response.json()[0]['unpublished'] == 0:
                break
        else:
            # Do some error handling here
            break


    # all segments are published... continue

else:
    print(response.status_code, response.text)








