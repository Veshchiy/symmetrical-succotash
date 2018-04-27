import MySQLdb
import re

def query_innodb_status(p_host, p_user, p_pwd, p_port=3306):
    # conn = MySQLdb.connect(host=p_host, user=p_user, passwd=p_pwd, database='information_schema', charset="utf8")
    conn = MySQLdb.connect(host=p_host, user=p_user, passwd=p_pwd, charset="utf8", port=p_port)
    query = "SHOW ENGINE INNODB STATUS;"
    cursor = conn.cursor()
    cursor.execute(query)
    result_list = []
    temp_list = cursor.fetchall()
    for op in temp_list:
        result_list = result_list + str(op).split("\\n")
    conn.close()
    return result_list


def deal_log(v_data):
    regexp_latest_fkey_error = re.compile("^LATEST FOREIGN KEY ERROR")
    regexp_latest_deadlock = re.compile("^LATEST DETECTED DEADLOCK")
    regexp_transactions = re.compile("^TRANSACTIONS")

    fkey_error_values = []
    deadlock_values = []
    fkey_error_exist = 0
    deadlock_exists = 0
    for line in v_data:
        if regexp_latest_fkey_error.match(line):
            fkey_error_exist = 1
        if regexp_latest_deadlock.match(line):
            fkey_error_exist = 0
            deadlock_exists = 1
        if regexp_transactions.match(line):
            break
        if fkey_error_exist:
            fkey_error_values.append(line)
        if deadlock_exists:
            deadlock_values.append(line)
    return fkey_error_values, deadlock_values


def cut_transaction(_iter, data, regex_start, regex_end):
    trans = 0
    trans_list = []
    for t in range(_iter, len(data)):
        if regex_start.match(data[t]):
            trans = 1
        if trans:
            trans_list.append(data[t])
        if regex_end.match(data[t]):
            _iter = t
            break
    return _iter, trans_list


def parse_transaction(data):
    details = {}
    line1 = data[0].strip('---').split(',')
    if len(line1[1].strip(' ').split(' ')) == 2:
        int_status = line1[1].strip(' ')
        time_sec = ''
        action = ''
    else:
        int_status = line1[1].strip(' ').split(' ')[0].strip(),
        time_sec = line1[1].strip(' ').split(' ')[1].strip(),
        action = ' '.join(line1[1].strip(' ').split(' ')[3:]).strip(),
    if len(data) != 2:
        line1 = data[0].split(',')
        line2 = data[1].split(',')
        line3 = data[2].split(',')
        line4 = data[3].split(',')
        details = {'tables_in_use': line2[0].strip('mysql tables in use').strip(),
                   'tables_locked': line2[1].strip(' locked').strip(),
                   'locked_structs': line3[0].strip(' lock struct(s)').strip(),
                   'heap_size': line3[1].strip(' heap size ').strip(),
                   'row_locks': line3[2].strip(' row lock(s)').strip(),
                   'undo_log_entries': line3[3].strip('undo log entries') if len(line3) == 4 else 0,
                   }
    else:
        line1 = data[0].split(',')
        line4 = data[3].split(',')

    transaction = {'id': line1[0].strip('TRANSACTION').strip(),
                   'int_status': int_status[0],
                   'time_sec': time_sec[0],
                   'action': action[0],
                   'details': details,
                   'MySQL_thread_id': line4[0].strip('MySQL thread id').strip(),
                   'OS_thread': line4[1].strip(' OS thread handle').strip(),
                   'query': {
                        'id': line4[2].strip(' ').split(' ')[2],
                        'ip': line4[2].strip(' ').split(' ')[3],
                        'user': line4[2].strip(' ').split(' ')[4],
                        'status': " ".join(line4[2].strip(' ').split(' ')[5:]),
                        'text': ''.join(data[4: len(data) - 2]),
                        },
                   'is_rolled_back': 0,
                   'waits_for_lock_to_be_granted': '',
                   'holds_the_locks': ''
                   }
    return transaction


def parse_fkey_error_block(v_dict_data):
    result = {'latest_foreign_key_error': {}}
    v_firstline = v_dict_data[2].split(' ')
    result['latest_foreign_key_error']['timestamp'] = {'val': v_firstline[0] + ' ' + v_firstline[1], 'unit': ''}
    result['latest_foreign_key_error']['os_thread_id'] = {'val': v_firstline[2], 'unit': 'ID'}
    if v_firstline[3] == 'Error':
        result['latest_foreign_key_error']['table_name'] = {'val': v_firstline[len(v_firstline) - 1].strip('.'),
                                                            'unit': ''}
        result['latest_foreign_key_error']['message'] = {'val': '\n'.join(v_dict_data[3:len(v_dict_data) - 1]), 'unit': 'TEXT'}
    elif v_firstline[3] == 'Transaction:':

        regex_start_transact = re.compile("^TRANSACTION")
        regex_end_trans = re.compile("^Foreign key constraint fails")

        v_iter, trans = cut_transaction(3, v_dict2, regex_start_transact, regex_end_trans)
        parsed_data = parse_transaction(trans)

        v_table = v_dict_data[v_iter].strip('Foreign key constraint fails for table ')

        result['latest_foreign_key_error']['transaction'] = {}
        result['latest_foreign_key_error']['transaction']['val'] = parsed_data
        result['latest_foreign_key_error']['transaction']['unit'] = ''

        result['latest_foreign_key_error']['schema_name'] = v_table.split('.')[0].strip('`'),
        result['latest_foreign_key_error']['table_name'] = v_table.split('.')[1].strip('`'),
        result['latest_foreign_key_error']['table_constraint'] = v_dict_data[v_iter + 2].strip()
    return result


def parse_deadlock_block(data):
    result = {'latest_deadlock': {}}
    v_firstline = data[2].split(' ')
    result['latest_deadlock']['timestamp'] = {'val': v_firstline[0] + ' ' + v_firstline[1], 'unit': ''}
    result['latest_deadlock']['os_thread_id'] = {'val': v_firstline[2], 'unit': 'ID'}

    regex_start_trans = re.compile("^TRANSACTION")
    regex_end_trans = re.compile("^RECORD LOCKS space")

    v_iter, trans1 = cut_transaction(3, data, regex_start_trans, regex_end_trans)
    parsed_data1 = parse_transaction(trans1)
    parsed_data1['waits_for_lock_to_be_granted'] = data[v_iter]

    v_iter1, trans2 = cut_transaction(v_iter+1, data, regex_start_trans, regex_end_trans)
    parsed_data2 = parse_transaction(trans2)
    parsed_data2['holds_the_locks'] = data[v_iter1]
    parsed_data1['waits_for_lock_to_be_granted'] = data[v_iter1 + 2]

    is_rollback = data[v_iter + 6].strip('*** WE ROLL BACK TRANSACTION (').strip(')')
    if is_rollback == '1':
        parsed_data1['is_rolled_back'] = 1
    elif is_rollback == '2':
        parsed_data1['is_rolled_back'] = 2

    result['latest_deadlock']['transaction1'] = {}
    result['latest_deadlock']['transaction1']['val'] = parsed_data1
    result['latest_deadlock']['transaction1']['unit'] = ''

    result['latest_deadlock']['transaction2'] = {}
    result['latest_deadlock']['transaction2']['val'] = parsed_data2
    result['latest_deadlock']['transaction2']['unit'] = ''

    return result


# def parse_transactions_block():
#     result = {'transactions': {}}
#     regex_start_trans_block = re.compile("LIST OF TRANSACTIONS FOR EACH SESSION:")
#     regex_stop_trans_block = re.compile("--------")
#     for



if __name__ == "__main__":
    result1 = {}
    result2 = {}
    v_innodb_status = query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="password", p_port=53306)
    # v_innodb_status = query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="pvB6gRBAz2xDbGnd", p_port=63306)
    v_dict1, v_dict2 = deal_log(v_innodb_status)
    if len(v_dict1):
        result1 = parse_fkey_error_block(v_dict1)

    if len(v_dict2):
        result2 = parse_deadlock_block(v_dict2)

    result1.update(result2)

    print
    for section in result1.keys():
        print
        print '[' + section + ']'
        for key in result1[section]:
            print key.rjust(10, ' ')
            if key in ('transaction', 'transaction1', 'transaction2'):
                for key_s in sorted(result1[section][key]['val'].keys()):
                    if key_s == 'query':
                        print ''.rjust(20, ' ') + key_s + ':'
                        for key_ss in sorted(result1[section][key]['val']['query'].keys()):
                            print ''.rjust(30, ' ') + key_ss + ': ' + result1[section][key]['val']['query'][key_ss]
                    else:
                        print ''.rjust(20, ' ') + key_s + ':' + ''.rjust(10, ' ') + str(result1[section][key]['val'][key_s])

            else:
                print result1[section][key]['val'] .rjust(20, ' ') \
                      + result1[section][key]['unit']
    print