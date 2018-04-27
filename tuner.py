import MySQLdb


class Tuner:

    def __init__(self):
        status_list = self.query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="pvB6gRBAz2xDbGnd",
                                               p_port=63306)
        variables_list = self.query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="pvB6gRBAz2xDbGnd",
                                                  p_port=63306)

    @staticmethod
    def query_innodb_status(p_host, p_user, p_pwd, p_port=3306):
        conn = MySQLdb.connect(host=p_host, user=p_user, passwd=p_pwd, charset="utf8", port=p_port)
        query = "SHOW GLOBAL STATUS;"
        cursor = conn.cursor()
        cursor.execute(query)
        temp_list = cursor.fetchall()
        result_list = dict((x, y) for x, y in temp_list)
        conn.close()
        return result_list

    @staticmethod
    def query_innodb_variables(self, p_host, p_user, p_pwd, p_port=3306):
        conn = MySQLdb.connect(host=p_host, user=p_user, passwd=p_pwd, charset="utf8", port=p_port)
        query = "SHOW GLOBAL VARIABLES;"
        cursor = conn.cursor()
        cursor.execute(query)
        temp_list = cursor.fetchall()
        result_list = dict((x, y) for x, y in temp_list)
        conn.close()
        return result_list



status_list = query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="pvB6gRBAz2xDbGnd", p_port=63306)
print status_list
for key in status_list.keys():
    print key + ': ' + status_list[key]
