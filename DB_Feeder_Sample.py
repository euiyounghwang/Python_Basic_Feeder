import json

# -------
import sys

# sys.path.append("/TOM/ES/")
sys.path.append("/ES/")

import jaydebeapi as jdb_api
import pandas as pd
import pandas.io.sql as pd_sql
from pandas import DataFrame
# noinspection PyShadowingNames
import pympler.asizeof
import os
import jpype
import inspect
import ES_Basic_Feeder.Utils.Util as Util
import schedule
import datetime
import time

ROOT_PATH = '/ES'


# noinspection PyMethodMayBeStatic,PyShadowingNames
class DB_Manager:

    def __init__(self, profile):
        """
        jar='/ES/ES_UnFair_Detection/lib/Reference_Library/ojdbc6.jar'
        args = '-Djava.class.path=%s' % jar

        jvm_path = jpype.getDefaultJVMPath()
        jpype.startJVM(jvm_path, args)
        :param profile:
        """

        self.profile = profile

        if str(self.profile).__eq__('dev'):
            self.is_debug_prod = False
        else:
            self.is_debug_prod = True

        # sys.path.append("/ES/")

        self.config_json = self.Initialized()

        self.conn = None
        self.sql = None

    def Initialized(self):
        """

        :return:
        """
        PROJECT_PATH = '/ES_Basic_Feeder/Setting/PROQ_Run_Setting.json'

        with open(ROOT_PATH + PROJECT_PATH, 'r+', encoding='utf-8') as f:
            jsonObject = json.load(f)

        root_json_object = jsonObject[self.profile]

        return root_json_object

    def Get_Connection(self):
        return self.conn

    def Set_Connection(self):
        """

        :return:
        """

        print(Util.bcolors().BOLD)
        print('---')
        print('Set_connection() Opend...[{}]'.format(self.is_debug_prod))
        print('---')
        print(Util.bcolors().ENDC)

        # if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
        #     jpype.attachThreadToJVM()
        #     jpype.java.lang.Thread.currentThread().setContextClassLoader(jpype.java.lang.ClassLoader.getSystemClassLoader())

        # SG_DB = self.config_json['interface']['master_sg_db'][0]
        SG_DB = self.config_json['interface']['sample_db'][0]

        self.conn = jdb_api.connect(SG_DB['class_name'],
                                    SG_DB['tns'],
                                    [SG_DB['user_id'], SG_DB['passwd']],
                                    SG_DB['jar_path'])

        self.sql = SG_DB['query']

        return self.conn

    def get_SQL(self):
        """

        :return:
        """

        return self.sql

    def Set_Disconnection(self, conn):
        """

        :param conn:
        :return:
        """
        if conn:
            if not conn._closed:
                conn.close()
                print(Util.bcolors().BOLD)
                print('---')
                print('Set_Disconnection() Closed...[{}]'.format(self.is_debug_prod))
                print('---')
                print(Util.bcolors().ENDC)


class SQL_Manager:

    def __init__(self):
        print('\nSQL Manager __init__')

        # self.Conn = DB_Manager().Set_Connection()

    def __del__(self):
        """

        :return:
        """
        print('\nSQL Manager __del__..')
        # DB_Manager().Set_Disconnection(self.Conn)

    def read_sql_http_post(self, conn, sql):
        """
        DB Record를 http_post 방식으로 색인
        :param conn:
        :param sql:
        :return:
        """

        df_all = pd_sql.read_sql_query(''.join(sql), conn, params=None)
        full_rows = []

        # pip3 install pyhdb
        if df_all.shape[0] > 0:
            print('read_sql_simple df_all.size = {}'.format(df_all.shape))  # shape : DataFrame의 행과 열 개수를 튜플로 반환
            # print('read_sql_simple df_all.size = {}'.format(df_all._get_values)) # shape : DataFrame의 행과 열 개수를 튜플로 반환
            # column info
            # for row in df_all:
            #     print(row)
            # print(len(df_all._get_values))
            # print(df_all, type(df_all))
            # print(df_all.values.tolist())
            # print('# KEY -> ', df_all.keys().tolist())
            # print('PD -> ', df_all['KEY'][0])
            # ---

            import ES_Basic_Feeder.Basic_Feeder as POST

            header = {'Content-Type': 'application/x-ndjson', 'Authorization': 'Basic xxx=='}
            # url = 'http://' + POST.Elastic_IP + '/_bulk?refresh=wait_for'
            url = 'http://' + POST.Elastic_IP + '/_bulk'

            if df_all.shape[0] > 0:
                docs = []
                # --
                # rows
                # --
                for loop in range(0, len(df_all._get_values)):
                    # ---
                    # columns
                    # --
                    each_row_columns = {}
                    for column in df_all.keys():
                        # print(loop, column, str(df_all.get(column)[loop]))
                        each_row_columns.update({column: str(df_all.get(column)[loop])})
                        # each_row_columns.update({'a': 'aa'}, )
                        each_row_columns.update({'TITLE': 'Feeder 샘플 데이터 색인 과제'}, )

                    docs.append(
                        [
                            # {'index': {'_index': POST.INDICS_NAME, '_type': '_doc', '_id': 'new_id_' + str(df_all.get('KEY')[loop])}},
                            {'index': {'_index': POST.INDICS_NAME, '_type': '_doc', '_id': 'new_id_' + str(loop)}},
                            each_row_columns,
                            # {'delete': {'_index': POST.INDICS_NAME, '_type': '_doc', '_id': 'new_id_0'}},
                        ]
                    )

                    print('\n db buffer -> ')
                    print(docs)

                    # --
                    # BUFFER 처리
                    # --

                    if POST.Get_Buffer_Length(docs) < POST.MEMORY_MAX_SIZE:
                        continue


                    POST.Elastic_INSERT_BULK(docs)


            # ---



        # ---
        # Total Count
        # ---
        print(Util.bcolors().BOLD + Util.bcolors().YELLOW)
        print('---')
        print('# feed_http_request_total_count -> ', POST.http_requests_total_count)
        print('# feed_success_total_count -> ', POST.feed_success_total_count)
        print('---')
        print(Util.bcolors().ENDC)

        # ---
        # HTTP POST Search
        # ---
        POST.HTTP_SEARCH(header, POST.INDICS_NAME)

    def read_sql(self, conn, sql):
        """
        DB Record를 row 전체저장
        :param conn:
        :param sql:
        :return:
        """

        df_all = pd_sql.read_sql_query(''.join(sql), conn, params=None)
        full_rows = []

        # pip3 install pyhdb
        if df_all.shape[0] > 0:
            print('read_sql_simple df_all.size = {}'.format(df_all.shape))  # shape : DataFrame의 행과 열 개수를 튜플로 반환
            # print('read_sql_simple df_all.size = {}'.format(df_all._get_values)) # shape : DataFrame의 행과 열 개수를 튜플로 반환
            # column info
            # for row in df_all:
            #     print(row)
            # print(len(df_all._get_values))
            # ---
            if df_all.shape[0] > 0:
                for loop in range(0, len(df_all._get_values)):
                    each_rows = []
                    for column in df_all.keys():
                        # print(loop, column, str(df_all.get(column)[loop]))
                        each_rows.append(str(df_all.get(column)[loop]))

                    full_rows.append(each_rows)
            # ---

        # ---
        # print(Util.bcolors().BOLD + Util.bcolors().YELLOW)
        # print('\n# each_rows -> ', full_rows)
        # print(Util.bcolors().ENDC)
        # ---

        return full_rows


class Run():

    def __init__(self, profile):
        print('\nRun __init__')

        self.profile = profile

        if str(self.profile).__eq__('dev'):
            self.description = '# 테스트계(TCEPRO) DB 기준'
        else:
            self.description = '# 가동계(PCEPRO) DB 기준'

        # ---
        # 미분류그룹
        # ---
        self.UNKNOWN = 'Unknown'

    def Invoke(self):
        """

        :return:
        """

        Manager, OBJ_CONN = None, None

        try:
            print('\nDB Manager')
            Manager = DB_Manager(self.profile)
            OBJ_CONN = Manager.Set_Connection()

            '''
            # ---
            # DB Record 전체 읽어와서 저장된 list 리턴
            # [['A'],...['Z']] 포맷으로 list 응답
            full_rows = [','.join(row) for row in SQL_Manager().read_sql(OBJ_CONN, Manager.get_SQL())]
            full_rows.insert(0, self.description)
            full_rows.insert(1, self.UNKNOWN)

            # ---
            print(Util.bcolors().BOLD + Util.bcolors().YELLOW)
            print('\n# each_rows -> ', full_rows)
            print(Util.bcolors().ENDC)
            # ---
            '''

            # ---
            # HTTP POST
            # --
            SQL_Manager().read_sql_http_post(OBJ_CONN, Manager.get_SQL())





        except Exception as ex:
            print('\nex -> ', ex)

        finally:
            # ---
            # MASTER CODE File Write
            # ---
            Manager.Set_Disconnection(OBJ_CONN)


if __name__ == '__main__':
    print('\n')

    # ---
    # DB 개발계, 가동계 여부
    Profile = 'dev'
    # Profile = 'prd'
    # ---

    Run(Profile).Invoke()




