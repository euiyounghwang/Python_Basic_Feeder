from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

Elastic_IP = "http://x.x.x.x:9210"

# 일레스틱서치 IP주소와 포트(기본:9200)로 연결한다
es = Elasticsearch(Elastic_IP, http_auth=('elastic', 'x'), ) # 환경에 맞게 바꿀 것
print('\n--')
print(json.dumps(es.info(), indent=4))

INDEX_FILE = '/ES/ES_Basic_Feeder/INPUT/index.json'

print('\n--')
# 인덱스는 독립된 파일 집합으로 관리되는 데이터 덩어리이다
def make_index(es, index_name):
    """인덱스를 신규 생성한다(존재하면 삭제 후 생성) """
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    with open(INDEX_FILE) as index_file:
        source = index_file.read().strip()
        print(es.indices.create(index=index_name, body=source))

    # print(es.indices.create(index=index_name))

index_name = 'goods'
make_index(es, index_name) # 상품 데이터 덩어리(인덱스)를 생성한다

print('\n--')
# 데이터를 저장한다
doc1 = {'goods_name': '삼성 노트북 9',    'price': 1000000}
doc2 = {'goods_name': '엘지 노트북 그램', 'price': 2000000}
doc3 = {'goods_name': '애플 맥북 프로',   'price': 3000000}
print('#1 : {}'.format(es.index(index=index_name, doc_type='string', body=doc1)))
print('#2 : {}'.format(es.index(index=index_name, doc_type='string', body=doc2)))
print('#3 : {}'.format(es.index(index=index_name, doc_type='string', body=doc3)))
es.indices.refresh(index=index_name)

print('\n--')
# 상품명에 '노트북'을 검색한다
results = es.search(index=index_name, body={'from':0, 'size':10, 'query':{'match':{'goods_name':'노트북'}}})
for result in results['hits']['hits']:
    print('score:', result['_score'], 'source:', result['_source'])