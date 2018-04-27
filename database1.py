
import mysql.connector
import re

cnx = mysql.connector.connect(user='root',
                              passwd='password',
                              database='trdsedb',
                              host='mta-qa-10-1-23.c43o7kg25r99.us-east-1.rds.amazonaws.com',
                              port=3306)
# cursor = cnx.cursor()
# query = "select distinct lower(replace(StagingTable, 'staging.', '')) from t_dserecordfielddefs where IsActive = 1"
# cursor.execute(query)
# result = cursor.fetchall()
# staging_tables = [str(list(i)[0]) for i in result]
# cursor.close()

query = 'show tables'
cursor = cnx.cursor()
cursor.execute(query)
result = cursor.fetchall()
all_tables = [str(list(i)[0]) for i in result]
cursor.close()

print len(all_tables)  # 1084359
                       # 1084359
                       # 1084266
                       # 1082684
                       # 1011079
                       # 702057
                       # 308876
                       # 219787

noquid = re.compile('NoQID_tmp_prepared_step1')
# b = [x for x in all_tables if not noquid.match(x)]
# res = [y for y in b if y not in staging_tables]

b = sorted([x for x in all_tables if noquid.match(x)], reverse=True)

for x in b:
    try:
        query = 'drop table if exists ' + x
        cursor = cnx.cursor()
        cursor.execute(query)
        cursor.close()
    except mysql.connector.errors.ProgrammingError as e:
        print e.message
    finally:
        cursor.close()

# query = 'select name, type from mysql.proc where db = "trdsedb"'
# cursor = cnx.cursor()
# cursor.execute(query)
# result = cursor.fetchall()
# procedures_list = [list(i) for i in result]
# cursor.close()

# print procedures_list
#
# # query = 'show create ' + procedures_list[1] + ' ' + procedures_list[1]
# query = 'show create function fn_createTempTable'
# cursor = cnx.cursor()
# cursor.execute(query)
# result = cursor.fetchall()
# proc = list(list(result)[0])[2]
# cursor.close()
#
# print proc

# all_proc_body = ''
# for proc in procedures_list:
#     query = 'show create ' + proc[1] + ' ' + proc[0]
#     cursor = cnx.cursor()
#     cursor.execute(query)
#     result = cursor.fetchall()
#     proc_body = str(list(list(result)[0])[2]) + ';\n\n'
#     cursor.close()
#     all_proc_body += proc_body
#
# print all_proc_body