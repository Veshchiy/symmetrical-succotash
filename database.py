import MySQLdb
import re


# cnx = mysql.connector.connect(user='root',
#                               passwd='password',
#                               database='trdsedb',
#                               host='mta-qa-10-1-23.c43o7kg25r99.us-east-1.rds.amazonaws.com',
#                               port=3306)
# # cursor = cnx.cursor()
# # query = "select distinct lower(replace(StagingTable, 'staging.', '')) from t_dserecordfielddefs where IsActive = 1"
# # cursor.execute(query)
# # result = cursor.fetchall()
# # staging_tables = [str(list(i)[0]) for i in result]
# # cursor.close()
#
# query = 'show tables'
# cursor = cnx.cursor()
# cursor.execute(query)
# result = cursor.fetchall()
# all_tables = [str(list(i)[0]) for i in result]
# cursor.close()
#
# print len(all_tables)  # 1084359
#                        # 1084359
#                        # 1084266
#                        # 1082684
#                        # 1010749
#
# noquid = re.compile('NoQID_tmp_prepared_step1')
# # b = [x for x in all_tables if not noquid.match(x)]
# # res = [y for y in b if y not in staging_tables]
#
# b = sorted([x for x in all_tables if noquid.match(x)], reverse=False)
#
# for x in b:
#     try:
#         query = 'drop table if exists ' + x
#         cursor = cnx.cursor()
#         cursor.execute(query)
#         cursor.close()
#     except mysql.connector.errors.ProgrammingError as e:
#         print e.message
#     finally:
#         cursor.close()



