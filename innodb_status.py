#!/usr/bin/python
import re
import MySQLdb
import sys
import getopt

NUM_REG = '\s+\d+.?\d*\s+'
DATE_REG = '\d{4}-\d{2}-\d{2}'
TIME_REG = '\d{2}:\d{2}:\d{2}'
STR_REG = '[A-z0-9]*'
WORD_REG = '\s\w+\s'
IP_REG = '\s+\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}[^0-9]\s+'

"""
INNODB_STATUS_DICT = {
    'section':{
            'key':['prefix','suffix','unit'], 
            'key':['prefix','suffix','unit']
    }
    PREFIX:
        ^ BEGIN 
    SUFFIX:
        $ END
    UNIT: BYTE,NUM,PER_SECOND...
          OPER ... CONTROL MARK
"""

INNODB_STATUS_DICT = {
    'bufferpool_memory': {
        # Total large memory allocated 549715968
        # Dictionary memory allocated 470460
        # Buffer pool size   32768
        # Free buffers       23188
        # Database pages     9484
        # Old database pages 3480
        # Modified db pages  0
        # Pending reads      0
        # Pending writes: LRU 0, flush list 0, single page 0
        # Pages made young 3067, not young 48190
        # 0.00 youngs/s, 0.00 non-youngs/s
        # Pages read 7721, created 1771, written 18370
        # Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
        # LRU len: 9484, unzip_LRU len: 0
        'total_large_memory_allocated': ['^Total large memory allocated ', '$', 'BYTE'],
        'dictionary_memory_allocated': ['^Dictionary memory allocated ', '$', 'BYTE'],
        'buffer_pool_size': ['^Buffer pool size ', '$', 'NUM'],
        'free_buffers': ['^Free buffers ', '$', 'NUM'],
        'database_pages': ['^Database pages ', '$', 'PAGE'],
        'old_database_pages': ['^Old database pages ', '$', 'PAGE'],
        'modified_db_pages': ['^Modified db pages ', '$', 'PAGE'],
        'pending_writes_lru': ['^Pending writes: LRU ', ',', 'NUM'],
        'pending_writes_flush_list': ['^Pending writes: LRU' + NUM_REG + ', flush list ', ',', 'NUM'],
        'pending_writes_single_page': ['^Pending writes: LRU' + NUM_REG + ', flush list' + NUM_REG + ', single page ',
                                       '$', 'NUM'],
        'pages_made_young': ['^Pages made young ', ',', 'NUM'],
        'pages_made_not_young': ['^Pages made young' + NUM_REG + ', not young', '$', 'NUM'],
        'pages_made_young_per_sec': ['^', 'youngs/s,', 'PER_SECOND'],
        'pages_made_non_young_per_sec': ['^' + NUM_REG + 'youngs/s,', 'non-youngs/s', 'PER_SECOND'],
        'pages_read': ['^Pages read ', ',', 'NUM'],
        'pages_created': ['^Pages read' + NUM_REG + ', created ', ',', 'NUM'],
        'pages_written': ['^Pages read' + NUM_REG + ', created' + NUM_REG + ', written ', '$', 'NUM'],
        'pages_read_ahead': ['^Pages read ahead ', '/s,', 'PER_SECOND'],
        'evicted_without_access': ['^Pages read ahead' + NUM_REG + '/s, evicted without access ', '/s', 'PER_SECOND'],
        'random_read_ahead': [
            '^Pages read ahead' + NUM_REG + '/s, evicted without access' + NUM_REG + '/s, Random read ahead ', '/s',
            'PER_SECOND'],
        'lrn_len': ['^LRU len: ', ',', 'NUM'],
        'unzip_lru_len': ['^LRU len:' + NUM_REG + ', unzip_LRU len: ', '$', 'NUM']
    },
    'log': {
        # Log sequence number 1283658707
        # Log flushed up to   1283658707
        # Pages flushed up to 1283658707
        # Last checkpoint at  1283658698
        # 0 pending log flushes, 0 pending chkp writes
        # 14438 log i/o's done, 0.00 log i/o's/second
        'log_sequence_number': ['^Log sequence number ', '$', 'NUM'],
        'log_flushed_up_to': ['^Log flushed up to ', '$', 'NUM'],
        'pages_flushed_up_to': ['^Pages flushed up to ', '$', 'NUM'],
        'last_checkpoint_at': ['^Last checkpoint at ', '$', 'NUM'],
        'pending_log_flushes': ['^', 'pending log flushes,', 'NUM'],
        'pending_chkp_writes': ['^' + NUM_REG + 'pending log flushes,', 'pending chkp writes$', 'NUM'],
        'log_ios_done': ['^', "log i/o's done,", 'NUM'],
        'log_ios_per_second': ["^'+NUM_REG+' log i/o's done,", "log i/o's/second$", 'NUM']
    },
    'row_operations': {
        # 0 queries inside InnoDB, 0 queries in queue
        # 0 read views open inside InnoDB
        # Process ID=12436, Main thread ID=140668680787712, state: sleeping
        # Number of rows inserted 16618, updated 50031, deleted 16618, read 7034937
        # 0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
        'queries_inside_innodb': ['^', 'queries inside InnoDB,', 'NUM'],
        'queries_in_queue': ["^'+NUM_REG+'queries inside InnoDB,", 'queries in queue$', 'NUM'],
        'read_views_open_inside_innodb': ['^', 'read views open inside InnoDB$', 'NUM'],
        'number_of_rows_inserted': ['^Number of rows inserted ', ',', 'NUM'],
        'number_of_rows_updated': ['^Number of rows inserted' + NUM_REG + ', updated ', ',', 'NUM'],
        'number_of_rows_deleted': ['^Number of rows inserted' + NUM_REG + ', updated' + NUM_REG + ', deleted ', ',',
                                   'NUM'],
        'number_of_rows_read': [
            '^Number of rows inserted' + NUM_REG + ', updated' + NUM_REG + ', deleted' + NUM_REG + ', read ', '$',
            'NUM'],
        'inserts_per_second': ['^', 'inserts/s,', 'NUM'],
        'updates_per_second': ['^' + NUM_REG + ' inserts/s, ', 'updates/s,', 'NUM'],
        'deletes_per_second': ['^' + NUM_REG + ' inserts/s, ' + NUM_REG + ' updates/s,', 'deletes/s,', 'NUM'],
        'reads_per_second': ['^' + NUM_REG + ' inserts/s, ' + NUM_REG + ' updates/s, ' + NUM_REG + ' deletes/s,',
                             'reads/s$', 'NUM']
    },
    'insert_buffer_adaptive_hash_index': {
        # Ibuf: size 1, free list len 0, seg size 2, 0 merges
        # merged operations:
        # insert 0, delete mark 0, delete 0
        # discarded operations:
        # insert 0, delete mark 0, delete 0
        # Hash table size 138389, node heap has 0 buffer(s)
        # ...
        # Hash table size 138389, node heap has 10 buffer(s)
        # 2535.04 hash searches/s, 2076.08 non-hash searches/s
        'ibuf_size': ['^Ibuf: size', ',', 'NUM'],
        'ibuf_free_list_len': ['^Ibuf: size ' + NUM_REG + ', free list len ', ',', 'NUM'],
        'ibuf_seg_size': ['^Ibuf: size ' + NUM_REG + ', free list len ' + NUM_REG + ', seg size ', ',', 'NUM'],
        'ibuf_merges': ['^Ibuf: size ' + NUM_REG + ', free list len ' + NUM_REG + ', seg size ' + NUM_REG + ', ',
                        'merges$', 'NUM'],
        'merged_operations': ['^merged operations:', '$', 'OPER'],
        '_insert_merged': ['^ insert ', ',', 'NUM'],
        '_delete_mark_merged': ['^ insert ' + NUM_REG + ', delete mark ', ',', 'NUM'],
        '_delete_merged': ['^ insert ' + NUM_REG + ', delete mark ' + NUM_REG + ', delete ', '$', 'NUM'],
        'discarded_operations': ['^discarded operations:', '$', 'OPER'],
        '_insert_discarded': ['^ insert ', ',', 'NUM'],
        '_delete_mark_discarded': ['^ insert ' + NUM_REG + ', delete mark ', ',', 'NUM'],
        '_delete_discarded': ['^ insert ' + NUM_REG + ', delete mark ' + NUM_REG + ', delete ', '$', 'NUM'],
        # 'merged_operations': ['^merged operations:', '$', 'OPER'],
        # 'merged_insert': ['^ insert ', ',', 'NUM'],
        # 'merged_delete_mark': ['^ insert ' + NUM_REG + ', delete mark ', ',', 'NUM'],
        # 'merged_delete': ['^ insert ' + NUM_REG + ', delete mark ' + NUM_REG + ', delete ', '$', 'NUM'],
        # 'discarded_operations': ['^discarded operations:', '$', 'OPER'],
        # 'discarded_insert': ['^ insert ', ',', 'NUM'],
        # 'discarded_delete_mark': ['^ insert ' + NUM_REG + ', delete mark ', ',', 'NUM'],
        # 'discarded_delete': ['^ insert ' + NUM_REG + ', delete mark ' + NUM_REG + ', delete ', '$', 'NUM'],
        'hash_searches_per_second': ['^', 'hash searches/s,', 'PER_SECOND'],
        'non_hash_searches_per_second': ['^' + NUM_REG + ' hash searches/s, ', 'non-hash searches/s$', 'PER_SECOND']
    },
    'file_io': {
        # I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
        # ...
        # I/O thread 17 state: waiting for completed aio requests (write thread)
        # Pending normal aio reads: [0, 0, 0, 0, 0, 0, 0, 0] , aio writes: [0, 0, 0, 0, 0, 0, 0, 0] ,
        # ibuf aio reads:, log i/o's:, sync i/o's:
        # Pending flushes (fsync) log: 0; buffer pool: 0
        # 2593 OS file reads, 28093 OS file writes, 530 OS fsyncs
        # 0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
        'os_file_reads': ['^', 'OS file reads,', 'NUM'],
        'os_file_writes': ['^' + NUM_REG + ' OS file reads, ', 'OS file writes,', 'NUM'],
        'os_fsyncs': ['^' + NUM_REG + ' OS file reads, ' + NUM_REG + ' OS file writes, ', 'OS fsyncs$', 'NUM'],
        'reads_per_second': ['^', 'reads/s,', 'NUM'],
        'avg_bytes_per_read': ['^' + NUM_REG + ' reads/s, ', 'avg bytes/read,', 'BYTE'],
        'writes_per_second': ['^' + NUM_REG + ' reads/s, ' + NUM_REG + ' avg bytes/read,', 'writes/s,', 'NUM'],
        'fsyncs_per_second': ['^' + NUM_REG + ' reads/s, ' + NUM_REG + ' avg bytes/read,' + NUM_REG + 'writes/s,',
                              'fsyncs/s$', 'NUM']
    },
    'transactions': {
        # Trx id counter 924584
        # Purge done for trx's n:o < 924584 undo n:o < 0 state: running but idle
        # History list length 610
        'trx_counter': ['^Trx id counter', '$', 'NUM'],
        'purge_done_for_trx': ["^Purge done for trx's n:o <", 'undo n:o', 'NUM'],
        'purge_done_for_undo': ["^Purge done for trx's n:o <" + NUM_REG + ' undo n:o <', 'state:', 'NUM'],
        'history_list_length': ['^History list length', '$', 'NUM']
    },
    'semaphores': {
        # OS WAIT ARRAY INFO: reservation count 26272
        # OS WAIT ARRAY INFO: signal count 26035
        # RW-shared spins 0, rounds 12106, OS waits 2966
        # RW-excl spins 0, rounds 35158, OS waits 2424
        # RW-sx spins 7, rounds 210, OS waits 7
        # Spin rounds per wait: 12106.00 RW-shared, 35158.00 RW-excl, 30.00 RW-sx
        'reservation_count': ['^OS WAIT ARRAY INFO: reservation count', '$', 'NUM'],
        'signal_count': ['^OS WAIT ARRAY INFO: signal count', '$', 'NUM'],
        'rw_shared_spins': ['^RW-shared spins', ',', 'NUM'],
        'rw_shared_rounds': ['^RW-shared spins ' + NUM_REG + ', rounds', ',', 'NUM'],
        'rw_shared_os_waits': ['^RW-shared spins ' + NUM_REG + ', rounds ' + NUM_REG + ' OS waits', '$', 'NUM'],
        'rw_excl_spins': ['^RW-excl spins', ',', 'NUM'],
        'rw_excl_rounds': ['^RW-excl spins ' + NUM_REG + ', rounds', ',', 'NUM'],
        'rw_excl_os_waits': ['^RW-excl spins ' + NUM_REG + ', rounds ' + NUM_REG + ' OS waits', '$', 'NUM'],
        'rw_sx_spins': ['^RW-sx spins', ',', 'NUM'],
        'rw_sx_rounds': ['^RW-sx spins ' + NUM_REG + ', rounds', ',', 'NUM'],
        'rw_sx_os_waits': ['^RW-sx spins ' + NUM_REG + ', rounds ' + NUM_REG + ' OS waits', '$', 'NUM'],
        'spin_rounds_per_wait_rw_shared': ['^Spin rounds per wait:', 'RW-shared,', 'NUM'],
        'spin_rounds_per_wait_rw_excl': ['^Spin rounds per wait:' + NUM_REG + ' RW-shared, ', 'RW-excl,', 'NUM'],
        'spin_rounds_per_wait_rw_sx': ['^Spin rounds per wait:' + NUM_REG + ' RW-shared, ' + NUM_REG + ' RW-excl, ',
                                       'RW-sx$', 'NUM']
    },
    'background_thread': {
        # srv_master_thread loops: 147 srv_active, 0 srv_shutdown, 19300 srv_idle
        # srv_master_thread log flush and writes: 19447
        'master_thread_loops_active': ['^srv_master_thread loops:', 'srv_active,', 'NUM'],
        'master_thread_loops_shutdown': ['^srv_master_thread loops: ' + NUM_REG + ' srv_active,', 'srv_shutdown,',
                                         'NUM'],
        'master_thread_loops_idle': [
            '^srv_master_thread loops: ' + NUM_REG + ' srv_active, ' + NUM_REG + 'srv_shutdown,', 'srv_idle$', 'NUM'],
        'master_thread_log_flush_and_writes': ['^srv_master_thread log flush and writes:', '$', 'NUM']
    },
    'latest_foreign_key_error': {
        # 2018-04-19 18:12:23 2b1bad2f6700 Transaction:
        # TRANSACTION 2865972098, ACTIVE 0 sec inserting
        # mysql tables in use 1, locked 1
        # 7 lock struct(s), heap size 1184, 3 row lock(s), undo log entries 1
        # MySQL thread id 	24152, OS thread handle 0x2b1bad2f6700, query id 3780908 10.99.175.218 root update
        # INSERT INTO t_document(Name, DocumentStoreObjectID, MimeType, Version, CreatedOn, DocumentReportID,
        # DocumentType, `Size`, Hash, UserID, ClientID, ProductID) VALUES ('name', '123', 'pdf', CURRENT_TIME,
        # CURRENT_TIME, 26364, 'document', 0, '', 90, 1, 0)
        # Foreign key constraint fails for table `tca`.`t_document`:
        # ,
        #   CONSTRAINT `FK_t_document_t_product` FOREIGN KEY (`ProductID`) REFERENCES `t_product` (`ProductID`)

        'os_datetime': ['^', '\w+\sTransaction:$', '', DATE_REG + ' ' + TIME_REG],
        'transaction_id': ['^TRANSACTION', ',', 'NUM', NUM_REG],
        'trx_active_time': ['^TRANSACTION ' + NUM_REG + ', ACTIVE ', ' sec ', 'SEC', 'NUM_REG'],
        'tables_in_use': ['^mysql tables in use', ',', 'NUM', NUM_REG],
        'tables_locked': ['^mysql tables in use ' + NUM_REG + ', locked', '$', 'NUM', NUM_REG],
        'locked_structs': ['^', 'lock', 'NUM', NUM_REG],
        'heap_size': ['^' + NUM_REG + ' lock struct\(s\), heap size ', ',', 'NUM', NUM_REG],
        'row_locks': ['^' + NUM_REG + ' lock struct\(s\), heap size ' + NUM_REG + ', ', 'row lock\(s\),', 'NUM', NUM_REG],
        'undo_log_entries': ['^' + NUM_REG + ' lock struct\(s\), heap size ' + NUM_REG + ', ' + NUM_REG +
                             ' row lock\(s\), undo log entries ', '$', 'NUM', NUM_REG],
        'mysql_thread_id': ['^MySQL thread id ', ',', 'NUM', NUM_REG],
        'os_thread_handle': ['^MySQL thread id ' + NUM_REG + ', OS thread handle ', ',', 'ID', STR_REG],
        'transaction_header': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle [A-z0-9]*, query id \d* \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}[^0-9]\s?\w+\s\w+\n\w.+[\n.+]+', 'Foreign', '', ''],
        'transaction_header1': {
            'mysql_thread_id': ['^MySQL thread id ', ',', 'NUM', NUM_REG],
            'os_thread_handle': ['^MySQL thread id ' + NUM_REG + ', OS thread handle ', ',', 'ID', STR_REG],
            'query_id': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle [A-z0-9]*, query id', '', 'ID', NUM_REG],
            'ip': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle [A-z0-9]*, query id ' + NUM_REG, '', 'IP', IP_REG],
            'user': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle ' + STR_REG + ', query id ' + NUM_REG + IP_REG, '', 'USER', 'WORD_REG'],
            'status': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle ' + STR_REG + ', query id ' + NUM_REG + IP_REG + WORD_REG, '', 'STATUS', 'WORD_REG'],
            'query_text': ['MySQL thread id\s*\d+.?\d*\s*, OS thread handle [A-z0-9]*, query id \d* \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}[^0-9]\s?\w+\s\w+\n\w.+[\n.+]+', 'Foreign', 'STR', '\n\w.+[\n.+]+Foreign']
        },
        'table': ['^Foreign key constraint fails for table', ':', 'STRING'],
        'query_str': ['^Foreign key constraint fails for table \w+ :\n', ]

    },
}


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
    """
        RETURN STRUCTURE
        {
            'section':
            {
                'key':{'val':'xxx','unit':'xxx'}
            }
        }
        example:
        {
            'bufferpool_memory':
            {
                'buffer_pool_size': {'unit': 'BYTE', 'val': '32768'},
                'database_pages': {'unit': 'PAGE', 'val': '9484'},
                'dictionary_memory_allocated': {'unit': 'BYTE'}
            }
        }
    """

    result_dict = {}
    regex_background_thread = re.compile("^BACKGROUND THREAD")
    regex_semaphores = re.compile("^SEMAPHORES")
    regexp_latest_fkey_error = re.compile("^LATEST FOREIGN KEY ERROR")
    regexp_latest_deadlock = re.compile("^LATEST DETECTED DEADLOCK")
    regex_transactions = re.compile("^TRANSACTIONS")
    regex_file_io = re.compile("^FILE I/O")
    regex_ins_buf_ahi = re.compile("^INSERT BUFFER AND ADAPTIVE HASH INDEX")
    regex_log = re.compile("^LOG")
    regex_bufferpool_memory = re.compile("^BUFFER POOL AND MEMORY")
    regex_row_operations = re.compile("^ROW OPERATIONS")
    regex_end = re.compile("^END OF INNODB MONITOR OUTPUT")

    cur_section = 'other'
    key_prefix = ''
    for line in v_data:
        if regex_background_thread.match(line):
            cur_section = 'background_thread'
        elif regex_semaphores.match(line):
            cur_section = 'semaphores'
        elif regexp_latest_fkey_error.match(line):
            cur_section = 'latest_foreign_key_error'
        elif regexp_latest_deadlock.match(line):
            cur_section = 'latest_deadlock'
        elif regex_transactions.match(line):
            cur_section = 'transactions'
        elif regex_file_io.match(line):
            cur_section = 'file_io'
        elif regex_ins_buf_ahi.match(line):
            cur_section = 'insert_buffer_adaptive_hash_index'
        elif regex_log.match(line):
            cur_section = 'log'
        elif regex_bufferpool_memory.match(line):
            cur_section = 'bufferpool_memory'
        elif regex_row_operations.match(line):
            cur_section = 'row_operations'
        elif regex_end.match(line):
            cur_section = 'other'
        else:
            if cur_section in INNODB_STATUS_DICT.keys():
                for item in INNODB_STATUS_DICT[cur_section].keys():
                    re_str = INNODB_STATUS_DICT[cur_section][item][0] + '' + \
                             INNODB_STATUS_DICT[cur_section][item][3] + '' + \
                             INNODB_STATUS_DICT[cur_section][item][1]

                    if cur_section == 'insert_buffer_adaptive_hash_index' and re.findall("merged operations", line):
                        key_prefix = 'merged_operations'
                        break
                    if cur_section == 'insert_buffer_adaptive_hash_index' and re.findall("discarded operations", line):
                        key_prefix = 'discarded_operations'
                        break
                    if cur_section == 'insert_buffer_adaptive_hash_index' and re.findall("Hash table size", line):
                        key_prefix = ''

                    if re.findall(re_str, line):
                        if INNODB_STATUS_DICT[cur_section][item][1] == "$":
                            val = re.split(INNODB_STATUS_DICT[cur_section][item][0], line)[-1]
                        else:
                            val = re.split(INNODB_STATUS_DICT[cur_section][item][1],
                                           re.split(INNODB_STATUS_DICT[cur_section][item][0], line)[-1])[0]
                        if (cur_section in result_dict.keys()) == 0:
                            result_dict[cur_section] = {}
                        if key_prefix == '':
                            result_dict[cur_section][item] = {'val': val.strip(),
                                                              'unit': INNODB_STATUS_DICT[cur_section][item][2]}
                        else:
                            result_dict[cur_section][key_prefix + item] = {
                                'val': val.strip(),
                                'unit': INNODB_STATUS_DICT[cur_section][item][2]
                            }
    return result_dict


def print_help():
    print "Usage:"
    print "./mysql_innodb_status.py -h <database_ip> -u <username> -p <password> -s<section>"
    print "    -h : database ip address/domain name"
    print "    -u : username"
    print "    -p : password"
    print "    -s : section"


if __name__ == "__main__":
    db = ''
    username = ''
    pwd = ''
    sect = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:u:p:s:")
        for o, v in opts:
            if o == "-h":
                db = v
            elif o == "-u":
                username = v
            elif o == "-p":
                pwd = v
            elif o == "-s":
                sect = v
    except getopt.GetoptError, msg:
        print_help()
        exit()

    # v_innodb_status = query_innodb_status(db, username, pwd)
    # v_innodb_status = query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="password", p_port=53306)
    v_innodb_status = query_innodb_status(p_host="127.0.0.1", p_user="root", p_pwd="pvB6gRBAz2xDbGnd", p_port=63306)

    v_dict = deal_log(v_innodb_status)


    sect = 'latest_foreign_key_error'
    print v_dict.keys()

    if sect in v_dict.keys():
        print
        print '[', sect, ']'
        print '-' * 90
        for key in sorted(v_dict[sect]):
            # print key,v_dict[section][key]['val'],v_dict[section][key]['unit']
            print key.ljust(50), v_dict[sect][key]['val'].rjust(20), v_dict[sect][key]['unit'].rjust(15)
    elif sect == '':
        # for section in sorted(v_dict.keys()):
        for section in v_dict.keys():
            print
            print '[', section, ']'
            print '-' * 90
            for key in sorted(v_dict[section]):
                # print key,v_dict[section][key]['val'],v_dict[section][key]['unit']
                print key.ljust(50), v_dict[section][key]['val'].rjust(20), v_dict[section][key]['unit'].rjust(15)
