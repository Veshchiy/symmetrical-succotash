CREATE DATABASE dbms_monitor DEFAULT CHARSET UTF8;

DELIMITER ;;

DROP PROCEDURE IF EXISTS dbms_monitor.calc_per_thread_buffers;;
CREATE PROCEDURE dbms_monitor.calc_per_thread_buffers()
  BEGIN
    -- We will use following theory:
    -- (read_buffer + read_rnd_buffer + sort_buffer + join buffer ) * max_sessions = <memory usage for all sessions>
    DECLARE v_read_buffer, v_join_buffer, v_sort_buffer, v_read_rnd_buffer, v_max_connctions, result, v_thread_stack, v_bin_log_cache BIGINT;

    SELECT VARIABLE_VALUE INTO v_read_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'READ_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_join_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'JOIN_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_sort_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'SORT_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_read_rnd_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'READ_RND_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_max_connctions FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'MAX_CONNECTIONS';
    SELECT VARIABLE_VALUE INTO v_thread_stack FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'THREAD_STACK';
    SELECT VARIABLE_VALUE INTO v_bin_log_cache FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'BINLOG_CACHE_SIZE';
    -- TODO: include innodb_thread_pool

    SET result = (v_read_buffer + v_join_buffer + v_sort_buffer + v_read_rnd_buffer + v_thread_stack + v_bin_log_cache) * v_max_connctions;

    SELECT 'PER_THREAD_BUFFERS' as "METRIC", result as "DETAILS";
  END;;

DROP PROCEDURE IF EXISTS dbms_monitor.calc_global_buffers;;
CREATE PROCEDURE dbms_monitor.calc_global_buffers()
  BEGIN
    -- We will use following theary:
    -- (read_buffer + read_rnd_buffer + sort_buffer + join buffer ) + max_sessions
    DECLARE v_innodb_buffer_pool, v_addit_mem_pool, v_innodb_log_buffer, v_key_buffer, v_query_cache, result BIGINT;

--    SELECT VARIABLE_VALUE INTO v_innodb_buffer_pool FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'INNODB_BUFFER_POOL_SIZE';
    SELECT VARIABLE_VALUE INTO v_addit_mem_pool FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'INNODB_ADDITIONAL_MEM_POOL_SIZE';
    SELECT VARIABLE_VALUE INTO v_innodb_log_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'INNODB_LOG_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_key_buffer FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'KEY_BUFFER_SIZE';
    SELECT VARIABLE_VALUE INTO v_query_cache FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'QUERY_CACHE_SIZE';

    SET result = ifnull(v_innodb_buffer_pool, 0) + v_addit_mem_pool + v_innodb_log_buffer + v_key_buffer + v_query_cache;

    SELECT 'GLOBAL_BUFFERS' as "METRIC", result as "DETAILS";
  END;;


DROP PROCEDURE IF EXISTS dbms_monitor.check_slow_queries;;
CREATE PROCEDURE dbms_monitor.check_slow_queries()
  BEGIN
    DECLARE v_questions, v_slow_queries, v_slow_lanch_time INT;
    DECLARE v_slow_log_glag varchar(4);

    SELECT VARIABLE_VALUE INTO v_slow_log_glag FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'SLOW_QUERY_LOG';

    IF v_slow_log_glag = 'ON' THEN
      SELECT VARIABLE_VALUE INTO v_questions FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'QUESTIONS';
      SELECT VARIABLE_VALUE INTO v_slow_queries FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'SLOW_QUERIES';
      SELECT VARIABLE_VALUE INTO v_slow_lanch_time FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'SLOW_LAUNCH_TIME';

      SELECT 'SLOW_QUERIES' as "METRIC", concat(v_slow_queries/v_questions*100, ' persent of queries are longer than ', v_slow_lanch_time) as "DETAILS";
    ELSE
      SELECT 'SLOW_QUERIES' as "METRIC", 'The slow query log is NOT enabled' as "DETAILS";
    END IF;
  END;;

DROP PROCEDURE IF EXISTS dbms_monitor.check_table_scans;;
CREATE PROCEDURE dbms_monitor.check_table_scans()
  BEGIN
    DECLARE v_com_select, v_read_rnd_next BIGINT;

    SELECT VARIABLE_VALUE INTO v_com_select FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'COM_SELECT';
    SELECT VARIABLE_VALUE INTO v_read_rnd_next FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'HANDLER_READ_RND_NEXT';

    select 'FULL_TABLE_SCANS_RATIO' AS "METRIC", CONCAT(ROUND(v_read_rnd_next/v_com_select), ':1') AS "DETAILS";

  END;;

DROP PROCEDURE IF EXISTS dbms_monitor.check_myisam_table_locking;;
CREATE PROCEDURE dbms_monitor.check_myisam_table_locking()
  BEGIN
    DECLARE v_tab_lock_waited, v_tab_lock_imm, v_questions BIGINT;
    DECLARE v_imm_lock_miss_rate VARCHAR(255);
    DECLARE v_concur_insert VARCHAR(10);

    SELECT VARIABLE_VALUE INTO v_tab_lock_waited FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'TABLE_LOCKS_WAITED';
    SELECT VARIABLE_VALUE INTO v_tab_lock_imm FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'TABLE_LOCKS_IMMEDIATE';
    SELECT VARIABLE_VALUE INTO v_questions FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'QUESTIONS';

    SELECT VARIABLE_VALUE INTO v_concur_insert FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'CONCURRENT_INSERT';

    IF v_tab_lock_waited > 0 THEN
      SET v_imm_lock_miss_rate = CONCAT('1:', v_tab_lock_imm/v_tab_lock_waited);
    ELSE
      SET v_imm_lock_miss_rate = CONCAT('0:', v_questions);
    END IF;

    IF v_imm_lock_miss_rate < 5000 THEN
      IF (v_concur_insert = 'AUTO' OR v_concur_insert = 'NEVER') THEN
        SELECT 'LOCK_WAIT_RATIO' AS "METRIC",
          CONCAT(v_imm_lock_miss_rate, '; If you have a high concurrency of inserts on Dynamic row-length tables, consider setting ''concurrent_insert=ALWAYS''.') AS "DETAILS";
      ELSE
        SELECT 'LOCK_WAIT_RATIO' AS "METRIC", v_imm_lock_miss_rate AS "DETAILS";
      END IF;
    END IF;

  END;;

DROP PROCEDURE IF EXISTS dbms_monitor.check_table_cache;;
CREATE PROCEDURE dbms_monitor.check_table_cache()
  BEGIN
    DECLARE v_tab_count, v_tab_open_cache, v_tab_def_cache, v_open_tabs, v_opened_tabs, v_open_tab_defs BIGINT;
    DECLARE v_tab_cache_hit_rate, v_tab_cach_fill DOUBLE;

    SELECT VARIABLE_VALUE INTO v_tab_open_cache FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'TABLE_OPEN_CACHE';
    SELECT VARIABLE_VALUE INTO v_tab_def_cache FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'TABLE_DEFINITION_CACHE';

    SELECT VARIABLE_VALUE INTO v_open_tabs FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'OPEN_TABLES';
    SELECT VARIABLE_VALUE INTO v_opened_tabs FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'OPENED_TABLES';
    SELECT VARIABLE_VALUE INTO v_open_tab_defs FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'OPEN_TABLE_DEFINITIONS';

    SELECT COUNT(*) INTO v_tab_count FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE';

    IF(v_opened_tabs <> 0 ) THEN
      SET v_tab_cache_hit_rate = v_open_tabs * 100 / v_opened_tabs,
        v_tab_cach_fill = v_open_tabs * 100 / v_tab_open_cache;
    ELSEIF (v_opened_tabs = 0 ) THEN
      SET v_tab_cache_hit_rate = 100,
        v_tab_cach_fill = v_open_tabs * 100 / v_tab_open_cache;
    END IF;

    IF v_tab_open_cache <> 0 THEN
      select 'TABLE_OPEN_CACHE' as "METRIC", v_tab_open_cache as "DETAILS"
      UNION
      select 'TABLE_DEFINITION_CACHE' as "METRIC", v_tab_def_cache as "DETAILS";
    END IF;

    IF v_tab_count <> 0 THEN
      select 'TABLE_COUNT' as "METRIC", v_tab_count as "DETAILS";
    END IF;

    IF v_tab_cach_fill < 95 THEN
      select 'OPEN_TABLES' as "METRIC", v_open_tabs as "DETAILS"
      UNION
      select 'TABLE_OPEN_CACHE' as "METRIC", concat(v_tab_open_cache, '; OK') as "DETAILS";
    ELSEIF (v_tab_cache_hit_rate < 85 and v_tab_cach_fill > 95) THEN
      select 'OPEN_TABLES' as "METRIC", v_open_tabs as "DETAILS"
      UNION
      select 'TABLE_CACHE_HIT_RATE' as "METRIC", v_tab_cache_hit_rate as "DETAILS"
      UNION
      select 'TABLE_CACHE_FILL' as "METRIC", v_tab_cach_fill as "DETAILS"
      UNION
      select 'TABLE_OPEN_CACHE' as "METRIC", concat(v_tab_open_cache, '; SHOULD BE INCREASED') as "DETAILS";
    ELSE
      select 'TABLE_CACHE_HIT_RATE' as "METRIC", v_tab_cache_hit_rate as "DETAILS"
      UNION
      select 'TABLE_CACHE_FILL' as "METRIC", v_tab_cach_fill as "DETAILS"
      UNION
      select 'TABLE_OPEN_CACHE' as "METRIC", concat(v_tab_open_cache, '; OK') as "DETAILS";
    END IF;

    IF (v_tab_def_cache <> 0 and v_tab_def_cache < v_tab_count and v_tab_count >= 100) THEN
      select 'TABLE_COUNT' as "METRIC", v_tab_count as "DETAILS"
      UNION
      select 'TABLE_DEFINITION_CACHE' as "METRIC", concat(v_tab_def_cache, '; SHOULD BE INCREASED') as "DETAILS";
    END IF;
  END;;

 DROP PROCEDURE IF EXISTS dbms_monitor.p_ShowMetadataLockSummary;
CREATE PROCEDURE dbms_monitor.p_ShowMetadataLockSummary()
BEGIN
  DECLARE v_table_schema VARCHAR(64);
  DECLARE v_table_name VARCHAR(64);
  DECLARE v_id bigint;
  DECLARE v_time bigint;
  DECLARE v_info longtext;
  DECLARE v_curMdlCount INT DEFAULT 0;
  DECLARE v_curMdlCtr INT DEFAULT 0;
  DECLARE curMdl CURSOR FOR SELECT * FROM tmp_blocked_metadata;

  DROP TEMPORARY TABLE IF EXISTS tmp_blocked_metadata;
  CREATE TEMPORARY TABLE IF NOT EXISTS tmp_blocked_metadata (
      table_schema varchar(64),
	  table_name varchar(64),
	  id bigint,
	  time bigint,
	  info longtext,
	  PRIMARY KEY(table_schema, table_name)
  ) ENGINE = MEMORY;

  REPLACE INTO tmp_blocked_metadata(table_schema,table_name,id,time,info)
  SELECT mdl.OBJECT_SCHEMA, mdl.OBJECT_NAME, t.PROCESSLIST_ID, t.PROCESSLIST_TIME, t.PROCESSLIST_INFO
  FROM performance_schema.metadata_locks mdl
  JOIN performance_schema.threads t
    ON mdl.OWNER_THREAD_ID = t.THREAD_ID
  WHERE mdl.LOCK_STATUS='PENDING'
  and mdl.LOCK_TYPE='EXCLUSIVE'
  ORDER BY mdl.OBJECT_SCHEMA,mdl.OBJECT_NAME,t.PROCESSLIST_TIME ASC;

  OPEN curMdl;
  SET v_curMdlCount = (SELECT FOUND_ROWS());
  WHILE (v_curMdlCtr < v_curMdlCount) DO
    FETCH curMdl INTO v_table_schema, v_table_name, v_id, v_time, v_info;

    SELECT CONCAT_WS(' '
                    , 'PID'
                    , t.PROCESSLIST_ID
                    , 'has metadata lock on'
                    , CONCAT(mdl.OBJECT_SCHEMA, '.', mdl.OBJECT_NAME)
                    , 'with current state'
                    , CONCAT_WS('', '[' ,t.PROCESSLIST_STATE, ']')
                    , 'for', t.PROCESSLIST_TIME, 'seconds and is currently running'
                    , CONCAT_WS('' ,'[' , t.PROCESSLIST_INFO, ']')
                    ) AS "Process(es) that have the metadata lock"
    FROM performance_schema.metadata_locks mdl
    JOIN performance_schema.threads t
      ON t.THREAD_ID = mdl.OWNER_THREAD_ID
    WHERE mdl.LOCK_STATUS='GRANTED'
    AND mdl.OBJECT_SCHEMA = v_table_schema
    AND mdl.OBJECT_NAME = v_table_name
    AND mdl.OWNER_THREAD_ID NOT IN(SELECT mdl2.OWNER_THREAD_ID
                                   FROM performance_schema.metadata_locks mdl2
                                   WHERE mdl2.LOCK_STATUS='PENDING'
                                   AND mdl.OBJECT_SCHEMA = mdl2.OBJECT_SCHEMA
                                   AND mdl.OBJECT_NAME = mdl2.OBJECT_NAME);

    SELECT CONCAT_WS(' ', 'PID'
                     , id, 'has been waiting for metadata lock on'
                     , CONCAT(v_table_schema, '.', v_table_name)
                     , 'for', v_time, 'seconds to execute', CONCAT_WS('', '[', v_info, ']')
                     ) AS "Oldest process waiting for metadata lock";
    
    SET v_curMdlCtr = v_curMdlCtr + 1;
    
	  SELECT CONCAT_WS(' ', 'PID'
                    , t.PROCESSLIST_ID, 'has been waiting for metadata lock on'
                    , CONCAT(v_table_schema,'.', v_table_name), 'for'
                    , t.PROCESSLIST_TIME, 'seconds to execute'
                    , CONCAT_WS('', '[', t.PROCESSLIST_INFO, ']')
                    ) AS "Other queries waiting for metadata lock" 
    FROM performance_schema.metadata_locks mdl 
    JOIN performance_schema.threads t 
      ON t.THREAD_ID = mdl.OWNER_THREAD_ID 
    WHERE mdl.LOCK_STATUS='PENDING' 
    AND mdl.OBJECT_SCHEMA = v_table_schema 
    AND mdl.OBJECT_NAME = v_table_name 
    AND mdl.OWNER_THREAD_ID 
    AND t.PROCESSLIST_ID <> v_id;
	END WHILE;
  CLOSE curMdl;
  
  DROP TABLE tmp_blocked_metadata;
END;;
  
DROP FUNCTION IF EXISTS dbms_monitor.f_threshold;;
CREATE FUNCTION dbms_monitor.f_threshold() RETURNS INTEGER DETERMINISTIC NO SQL RETURN @threshold;;

DROP FUNCTION IF EXISTS dbms_monitor.check_innodb_log_size;
CREATE FUNCTION dbms_monitor.check_innodb_log_size()
  RETURNS DECIMAL(8,2)
BEGIN
  RETURN round((@@innodb_log_files_in_group * @@innodb_log_file_size)/1024/1024, 2);
END;

DELIMITER ;

DROP VIEW IF EXISTS dbms_monitor.session_wait_duration;
CREATE VIEW dbms_monitor.session_wait_duration AS
SELECT p.user,
      LEFT(p.HOST, LOCATE(':', p.HOST) - 1) host, p.id,
      TIMESTAMPDIFF(SECOND, t.TRX_STARTED, NOW()) duration,
      COUNT(DISTINCT ot.REQUESTING_TRX_ID) waiting
    FROM information_schema.INNODB_TRX t
    JOIN information_schema.PROCESSLIST p
      ON ( p.ID = t.TRX_MYSQL_THREAD_ID )
    LEFT JOIN information_schema.INNODB_LOCK_WAITS ot
      ON ( ot.BLOCKING_TRX_ID = t.TRX_id )
    WHERE t.TRX_STARTED + INTERVAL dbms_monitor.f_threshold() SECOND <= NOW()
    GROUP BY LEFT(p.HOST, LOCATE(':', p.HOST) - 1), p.id, duration
    HAVING duration >= dbms_monitor.f_threshold() OR waiting > 0;


-- set @threshold = 10;
-- select * from dbms_monitor.session_wait_duration;

DROP VIEW IF EXISTS dbms_monitor.session_blockings;
CREATE VIEW dbms_monitor.session_blockings AS
SELECT p1.id
      ,p1.user
      ,p1.state
      ,it1.trx_id              AS requesting_trx_id
      ,it1.trx_mysql_thread_id AS requesting_thread
      ,it1.trx_query           AS requesting_query
      ,it2.trx_id              AS blocking_trx_id
      ,it2.trx_mysql_thread_id AS blocking_thread
      ,it2.trx_query           AS blocking_query
FROM information_schema.INNODB_LOCK_WAITS ilw
INNER JOIN information_schema.INNODB_TRX it1
  ON ilw.requested_lock_id = it1.trx_id
INNER JOIN information_schema.PROCESSLIST p1
  ON p1.ID = it1.trx_mysql_thread_id
INNER JOIN information_schema.INNODB_TRX it2
  ON ilw.blocking_trx_id = it1.trx_id
INNER JOIN information_schema.PROCESSLIST p2
  ON p2.ID = it2.trx_mysql_thread_id;
 
DROP VIEW IF EXISTS dbms_monitor.user_connections;
CREATE VIEW dbms_monitor.user_connections AS
SELECT USER
    , CASE
        WHEN substring_index(HOST, ':', 1) LIKE '10.35.%'
          THEN 'APP(Amazon)'
        WHEN substring_index(HOST, ':', 1) LIKE '172.31.159.%'
          THEN 'LUXOFT'
        ELSE HOST END RESOURCE
    , count(1)                      AS SESSION_COUNT
  FROM information_schema.PROCESSLIST
  WHERE substring_index(HOST, ':', 1) <> 'localhost'
  GROUP BY USER, substring_index(HOST, ':', 1);

DROP VIEW IF EXISTS dbms_monitor.schema_weight_by_engine;
CREATE VIEW dbms_monitor.schema_weight_by_engine AS
SELECT  TABLE_SCHEMA, ENGINE,
        ROUND(SUM(data_length) /1024/1024, 1) AS "Data MB",
        ROUND(SUM(data_length) /1024/1024/1024, 1) AS "Data GB",
        ROUND(SUM(data_free) /1024/1024, 1) AS "Data_Free MB",
        ROUND(SUM(data_free) /1024/1024/1024, 1) AS "Data_Free GB",
        ROUND(SUM(index_length)/1024/1024, 1) AS "Index MB",
        ROUND(SUM(index_length)/1024/1024/1024, 1) AS "Index GB",
        ROUND(SUM(data_length + data_free + index_length)/1024/1024, 1) AS "Total MB",
        ROUND(SUM(data_length + data_free + index_length)/1024/1024/1024, 1) AS "Total GB",
        COUNT(*) "Num Tables"
    FROM  information_schema.TABLES
    WHERE  TABLE_SCHEMA not in ('information_schema', 'PERFORMANCE_SCHEMA', 'SYS_SCHEMA', 'mysql')
    AND table_type = 'BASE TABLE'
    GROUP BY  ENGINE, TABLE_SCHEMA
    order by 3,1,2;

DROP VIEW IF EXISTS dbms_monitor.schema_weight;
CREATE VIEW dbms_monitor.schema_weight AS
select TABLE_SCHEMA,
  sum(ifnull(`Data MB` + `Data_Free MB`, 0)) as DATA_MB,
  sum(ifnull(`Data GB` + `Data_Free MB`, 0)) as DATA_GB,
  sum(ifnull(`Index MB`, 0)) as INDEX_MB,
  sum(ifnull(`Index GB`, 0)) as INDEX_GB,
  sum(ifnull(`Total MB`, 0)) as TOTAL_MB,
  sum(ifnull(`Total GB`, 0)) as TOTAL_GB,
  sum(`Num Tables`) as TABLES_COUNT
from dbms_monitor.schema_weight_by_engine
group by TABLE_SCHEMA;

DROP VIEW IF EXISTS dbms_monitor.instance_weight;
CREATE VIEW dbms_monitor.instance_weight AS
SELECT
  sum(ifnull(`Data MB` + `Data_Free MB`, 0)) as DATA_MB,
  sum(ifnull(`Data GB` + `Data_Free MB`, 0)) as DATA_GB,
  sum(ifnull(`Index MB`, 0)) as INDEX_MB,
  sum(ifnull(`Index GB`, 0)) as INDEX_GB,
  dbms_monitor.check_innodb_log_size() INNODB_LOG_MB,
  round(dbms_monitor.check_innodb_log_size()/1024, 2) INNODB_LOG_GB,
  sum(ifnull(`Total MB`, 0)) + dbms_monitor.check_innodb_log_size() as TOTAL_MB,
  sum(ifnull(`Total GB`, 0)) + round(dbms_monitor.check_innodb_log_size()/1024, 2) as TOTAL_GB,
  sum(`Num Tables`) as TABLES_COUNT
from dbms_monitor.schema_weight_by_engine;
	
DROP VIEW IF EXISTS dbms_monitor.index_info;
CREATE VIEW dbms_monitor.index_info AS
   SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        NON_UNIQUE,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS IDX_PREFIX
    FROM
        information_schema.STATISTICS
    WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA')
    AND INDEX_TYPE='BTREE'
    GROUP BY
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        NON_UNIQUE;

DROP VIEW IF EXISTS dbms_monitor.index_overhead;
CREATE VIEW dbms_monitor.index_overhead as
SELECT
    CONCAT('The B-Tree NOT UNIQUE index ', idx1.IDX_PREFIX, ' is prefix of the composed index ', idx2.INDEX_NAME) AS Message,
    CONCAT(idx1.TABLE_SCHEMA, '.', idx1.TABLE_NAME) AS FULL_TABLE_NAME,
    idx1.INDEX_NAME,
    idx1.IDX_PREFIX,
    idx2.INDEX_NAME AS COMPOSED_INDEX_NAME,
    idx2.IDX_PREFIX AS COMPOSED_IDX_PREFIX
FROM dbms_monitor.index_info AS idx1
    INNER JOIN dbms_monitor.index_info AS idx2
    USING (TABLE_SCHEMA, TABLE_NAME)
    WHERE idx1.NON_UNIQUE = 1 AND idx1.IDX_PREFIX != idx2.IDX_PREFIX AND LOCATE(CONCAT(idx1.IDX_PREFIX, ','), idx2.IDX_PREFIX) = 1
;

DROP VIEW IF EXISTS dbms_monitor.tables_with_primary;
CREATE VIEW dbms_monitor.tables_with_primary AS
 SELECT t.TABLE_SCHEMA, t.TABLE_NAME
  FROM information_schema.COLUMNS c
    JOIN information_schema.TABLES t USING (TABLE_SCHEMA, TABLE_NAME)
  WHERE t.TABLE_SCHEMA not in ('mysql','sys','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA')
  AND c.COLUMN_KEY='PRI'
  GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME;

DROP VIEW IF EXISTS dbms_monitor.tables_without_primary;
CREATE VIEW dbms_monitor.tables_without_primary AS
SELECT
    t2.TABLE_SCHEMA,
    t2.TABLE_NAME,
    t2.ENGINE
FROM information_schema.TABLES t2
  LEFT JOIN dbms_monitor.tables_with_primary t USING(TABLE_SCHEMA, TABLE_NAME)
WHERE t2.TABLE_SCHEMA not in ('mysql','sys','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA')
AND t2.TABLE_TYPE = 'BASE TABLE'
AND t.TABLE_SCHEMA IS NULL
GROUP BY
    t2.TABLE_SCHEMA,
    t2.TABLE_NAME,
    t2.ENGINE;
	
DROP VIEW IF EXISTS dbms_monitor.tables_with_autoinc;
CREATE VIEW dbms_monitor.tables_with_autoinc AS
  SELECT
  t.TABLE_SCHEMA,
  t.TABLE_NAME,
  c.COLUMN_NAME,
  c.DATA_TYPE,
  c.COLUMN_TYPE,
  c.EXTRA,
  t.AUTO_INCREMENT,
(CASE c.DATA_TYPE
   WHEN 'tinyint' THEN 255
   WHEN 'smallint' THEN 65535
   WHEN 'mediumint' THEN 16777215
   WHEN 'int' THEN 4294967295
   WHEN 'bigint' THEN 18446744073709551615
END >> if(LOCATE('unsigned', c.COLUMN_TYPE) > 0, 0, 1)) as MAX_VALUE
FROM information_schema.TABLES t
  INNER JOIN information_schema.COLUMNS c USING(TABLE_SCHEMA, TABLE_NAME)
WHERE 1=1
AND c.EXTRA = 'auto_increment'
AND c.DATA_TYPE in ('tinyint', 'smallint', 'int', 'mediumint', 'bigint')
AND t.TABLE_SCHEMA NOT IN  ('mysql', 'information_schema', 'performance_schema','sys')
order by t.AUTO_INCREMENT desc;

DROP VIEW IF EXISTS dbms_monitor.autoinc_fill_pct;
CREATE VIEW dbms_monitor.autoinc_fill_pct AS
SELECT t.TABLE_SCHEMA,
  t.TABLE_NAME,
  t.COLUMN_NAME,
  t.DATA_TYPE,
  t.COLUMN_TYPE,
  t.AUTO_INCREMENT,
  t.MAX_VALUE,
  t.AUTO_INCREMENT/t.MAX_VALUE *100 as FILL_PCT
FROM dbms_monitor.tables_with_autoinc t
WHERE round(t.AUTO_INCREMENT/t.MAX_VALUE *100, 2) <> 0
ORDER BY FILL_PCT DESC;
