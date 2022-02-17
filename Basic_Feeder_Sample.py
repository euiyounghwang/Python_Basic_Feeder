import json

import requests
import datetime
import elasticsearch
from elasticsearch import helpers

import sys

# sys.path.append("D:\\Python\\")
from torch.distributions import Gamma

sys.path.append("/ES/")

import ES_Basic_Feeder.Utils.Util as Util


import ES_Basic_Feeder.Utils.Util as Util

# pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install pigar

# pip freeze > requirements.txt
# pip install -r requirements.txt

# pigar -p ./requirements.txt -P D:\Python\ES_Basic_Feeder\
# pigar -p /ES/ES_Basic_Feeder/requirements.txt -P /ES/ES_Basic_Feeder/

# Install
# pip --trusted-host pypi.org --trusted-host files.pythonhosted.org install -r .\requirements.txt


# ---
# MY CLUSTER
# ---
Elastic_IP = 'x.x.x.x:9201'


# INDICS_NAME = 'Sample'
INDICS_NAME = 'sample'
MEMORY_MAX_SIZE = 500

feed_success_total_count, feed_fail_total_count = 0, 0
http_requests_total_count = 0


# https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
# curl -XPOST -u elastic:x -H'Content-Type: application/x-ndjson' http://x.x.x.x:9201/_bulk?pretty --data-binary @/home/ECM_BULK_PROD/test_idx/test_idx_elasticsearch_results_201904251556197543.789248

'''
https://www.json-to-ndjson.app/
curl - XPOST "http://x.x.x.x:9201/_bulk" - H 'Content-Type: application/json' - -data - binary @/ES/Basic_Feeder/INPUT/posts.json
curl - XPOST - u elastic:x "http://x.x.x.x:9201/_bulk" - H 'Content-Type: application/json' - -data - binary @/ES/Basic_Feeder/INPUT/posts.json
'''

# POST _bulk
# {"index" : {"_index" : "test", "_type": "_doc", "_id" : "1"}}
# {"s" : "s",  "a":"a1"}
# {"update" : {"_index" : "test", "_type": "_doc", "_id" : "1"}}
# {"doc" : {"s": "s1"}, "doc_as_upsert" : true}


# POST _bulk
# {"update" : {"_index" : "test", "_type": "_doc", "_id" : "1"}}
# {"doc" : {"s": "s1"}, "doc_as_upsert" : false}



def Elastic_Ack(each_row):
    """

    :param each_row:
    :return:
    """

    global  feed_success_total_count
    global  feed_fail_total_count

    # print('\n Elastic_Ack', each_row, type(each_row))

    response_ack = []

    if 'index' in each_row:
        if str(each_row['index']['status']).__contains__('2'):
            response_ack.append('[CS] ' + each_row['index']['_id'])
            feed_success_total_count += 1
        else:
            # log.error('curl_file_command -> index' + ' >> ' + results)
            response_ack.append('[CF] ' + each_row['index']['_id'])
            feed_fail_total_count += 1

    elif 'update' in each_row:
        if str(each_row['update']['status']).__contains__('2'):
            response_ack.append('[US] ' + each_row['update']['_id'])
            feed_success_total_count += 1
        else:
            # log.error('curl_file_command -> index' + ' >> ' + results)
            response_ack.append('[UF] ' + each_row['update']['_id'])
            feed_fail_total_count += 1

    elif 'delete' in each_row:
        if str(each_row['delete']['status']).__contains__('2'):
            response_ack.append('[DS] ' + each_row['delete']['_id'])
            feed_success_total_count += 1
        else:
            # log.error('curl_file_command -> index' + ' >> ' + results)
            # print('@@@', each_row[1])
            response_ack.append('[DF] ' + each_row['delete']['_id'])
            feed_fail_total_count += 1

    return response_ack



def Get_Buffer_Length(docs):
    """

    :param docs:
    :return:
    """
    max_len = 0
    for doc in docs:
        max_len += len(str(doc))

    print('\n' + Util.bcolors().BOLD + Util.bcolors().YELLOW + 'StringBuffer [Add Meta] ' + str(max_len) + 'Bytes /' + str(MEMORY_MAX_SIZE) + 'Bytes (Total Meta Buffer Ratio : ' + str(round((float)(max_len / MEMORY_MAX_SIZE), 2) * 100) + '%)' + Util.bcolors().ENDC)

    return max_len



def HTTP_SEARCH(header, index_name):
    """
    Search all
    :param header:
    :param index_name:
    :return:
    """
    print(Util.bcolors().BOLD)
    print('---')
    url = 'http://' + Elastic_IP + '/' + index_name + '/_search'
    search_query = {
        "track_total_hits": True,
        "query": {
            "match_all": {}
        },
        "size": 1
    }
    results = requests.post(url=url, headers=header, data=json.dumps(search_query, ensure_ascii=False), timeout=30000)

    if results.status_code.__eq__(200):
        response_json = json.loads(results.text)
        print('# Search Results -> {}'.format(response_json['hits']['total']['value']))
        print(json.dumps(response_json, ensure_ascii=False, indent=4))
        print('---')
        print(Util.bcolors().ENDC)

    else:
        print('# Search Results -> {}'.format(results.text))



def Http_INSERT_BULK(url, header, loop, docs):
    """

    :param url:
    :param header:
    :param loop:
    :param docs:
    :return:
    """

    global http_requests_total_count

    print(Util.bcolors().BOLD)
    print('# Remain Send Buffer..')
    print(Util.bcolors().ENDC)

    Buffer = []

    # ---
    # 실제 record -> buffer 담기
    # ---
    # for doc in docs:
    #     for each_row in doc:
    #         Buffer.append(str(each_row).replace("'", '"') + '\n')

    # ---
    # 한줄로 작성가능
    Buffer = [str(each_row).replace("'", '"') + '\n' for doc in docs for each_row in doc]

    # 기존 실제데이터 버퍼(docs) 초기화
    docs.clear()

    print('# Buffer -> ', Buffer)
    results = requests.post(url=url, headers=header, data=''.join(Buffer).encode('utf-8'), timeout=30000)

    if results.status_code.__eq__(200):

        http_requests_total_count +=1

        print('\n')
        print('#' * 20)
        print('# [{}] Response_ack -> '.format(loop))
        print('# Response_status -> {}'.format(results.status_code))
        print(json.dumps(json.loads(results.text), indent=4))
        # print([Elastic_Ack(json.loads(rows)) for rows in [results.text]])
        items = json.loads(results.text)['items']
        response_ack = []
        for rows in items:
            response_each_row = ''.join(Elastic_Ack(json.loads(str(rows).replace("'", '"'))))
            response_ack.append(response_each_row)
            # print(response_ack)

        print(Util.bcolors().BOLD)
        print('#ACK -> {}'.format(','.join(response_ack)))
        print(Util.bcolors.ENDC)
        print('#' * 20)

        Buffer.clear()



def Http_INSERT():
    """
    curl -XPOST -u elastic:x -H'Content-Type: application/x-ndjson' http://x.x.x.x:9201/_bulk?pretty --data-binary @/home/ECM_BULK_PROD/test_idx/test_idx_elasticsearch_results_201904251556197543.789248
    http://jason-heo.github.io/elasticsearch/2016/07/16/elasticsearch-with-python.html
    :return:
    """
    header = {'Content-Type': 'application/x-ndjson', 'Authorization': 'Basic xxx=='}
    # url = 'http://' + Elastic_IP + '/_bulk?refresh=wait_for'
    url = 'http://' + Elastic_IP + '/_bulk'
    # print('url', url)

    docs = []
    Buffer = []

    global http_requests_total_count

    try:
        print("\n\n#####################################")
        start_time = datetime.datetime.now()
        print('StartTime ' + str(start_time))
        # print('params #1 -> ', json.dumps(docs, indent=3))

        loop = 0
        for cnt in range(20):
            docs.append(
                [
                    # {'index': { '_index': INDICS_NAME, '_type': '_doc', '_id': 'new_id_' + str(cnt)}},
                    # {'TITLE': 'Feeder 샘플 데이터 색인 과제'},
                    {'delete': {'_index': INDICS_NAME, '_type': '_doc', '_id': 'new_id_0'}},
                    # {'update': {'_index': INDICS_NAME, '_type': '_doc', '_id': 'new_id_' + str(cnt)}},
                    # {'doc' : {'TITLE': 'Feeder 샘플 데이터 색인 과제 변경'}},
                    {'update': {'_index': INDICS_NAME, '_type': '_doc', '_id': 'new_id_' + str(cnt)}},
                    {'doc': {'TITLE': 'Feeder 샘플 데이터 색인 과제 변경'}, 'doc_as_upsert': 'true'},
                ]
            )


            # ---
            # 실제 record -> buffer 담기
            # ---
            for doc in docs:
                for each_row in doc:
                    # print('# -> ', each_row)
                    Buffer.append(str(each_row).replace("'", '"') + '\n')
                    # Buffer.append(str(each_row[0]).replace("'", '"') + '\n' + str(each_row[1]).replace("'", '"') + '\n')

            # ---
            # 한줄로 작성가능
            # ---
            # Buffer = [str(each_row).replace("'", '"') + '\n' for doc in docs for each_row in doc]

            print('\n# docs ->')
            print(docs)


            # 기존 실제데이터 버퍼(docs) 초기화
            docs.clear()

            loop += 1
            print('\n# Buffer ->')
            print(Buffer)
            # print('\n# ''.join(Buffer) ->')
            # print(''.join(Buffer))
            # exit(1)
            results = requests.post(url=url, headers=header, data=''.join(Buffer).encode('utf-8'), timeout=30000)

            if results.status_code.__eq__(200):

                http_requests_total_count += 1

                print('\n')
                print('#' * 20)
                print('# [{}] Response_ack -> '.format(loop))
                print('# Response_status -> {}'.format(results.status_code))
                print(json.dumps(json.loads(results.text), indent=4))
                # print([Elastic_Ack(json.loads(rows)) for rows in [results.text]])

                # ---
                # SEARCH REQUEST ACK
                # ---

                items = json.loads(results.text)['items']
                response_ack = []
                for rows in items:
                    response_each_row = ''.join(Elastic_Ack(json.loads(str(rows).replace("'",'"'))))
                    response_ack.append(response_each_row)

                print(Util.bcolors().BOLD)
                print('#ACK -> {}'.format(','.join(response_ack)))
                print(Util.bcolors.ENDC)
                print('#' * 20)

                # ---
                # 전송후 성공 -> 버퍼 초기화
                # ---
                Buffer.clear()


        # ---
        # Total Count
        # ---
        print(Util.bcolors().BOLD + Util.bcolors().YELLOW)
        print('---')
        print('# feed_http_request_total_count -> ', http_requests_total_count)
        print('# feed_success_total_count -> ', feed_success_total_count)
        print('---')
        print(Util.bcolors().ENDC)


    except Exception as ex:
        print('Exception -> ', ex)

    finally:

        # ---
        # HTTP POST SEARCH Sample
        # ---
        HTTP_SEARCH(header, INDICS_NAME)

        return


if __name__ == '__main__':

    # ---
    # HTTP POST METHOD
    # ---
    Http_INSERT()


