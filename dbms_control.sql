CREATE DATABASE dbms_control DEFAULT CHARSET UTF8;

DELIMITER ;;

DROP PROCEDURE IF EXISTS dbms_control.kill_long_sessions;;
CREATE PROCEDURE dbms_control.kill_long_sessions( IN runtime TINYINT UNSIGNED )
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE connid INT UNSIGNED;
  DECLARE cur1 CURSOR FOR SELECT ID
                          FROM information_schema.PROCESSLIST p
                          JOIN information_schema.USER_PRIVILEGES up
                            ON CONCAT ("'", p.USER, "'@'", SUBSTRING_INDEX(p.HOST,':',1), "'") = up.GRANTEE
                          WHERE p.COMMAND ='Query'
                          AND p.TIME >= runtime
                          AND up.GRANTEE NOT IN (SELECT GRANTEE FROM information_schema.USER_PRIVILEGES
                                                  WHERE PRIVILEGE_TYPE IN ('SUPER', 'REPLICATION CLIENT', 'REPLICATION SLAVE')
                                                  GROUP BY GRANTEE);
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;

  OPEN cur1;
  FETCH cur1 INTO connid;
  WHILE NOT done DO
     SET @s = CONCAT('KILL ', connid);
     PREPARE stmt FROM @s;
     EXECUTE stmt;
     DEALLOCATE PREPARE stmt;
     FETCH cur1 INTO connid;
  END WHILE;
  CLOSE cur1;
END;;

DROP PROCEDURE IF EXISTS dbms_control.shrink_table;;
CREATE PROCEDURE dbms_control.shrink_table(v_schema_name varchar(64), v_table_name varchar(64))
  BEGIN
    DECLARE sql_error CONDITION FOR SQLSTATE '42000';
    DECLARE EXIT HANDLER FOR sql_error select @v_sql;

    IF(SELECT gv.VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES gv WHERE gv.VARIABLE_NAME = 'INNODB_FILE_PER_TABLE') = 'ON'
      THEN
        SET @v_sql = concat('OPTIMIZE TABLE ', v_schema_name, '.', v_table_name);
        PREPARE stmt FROM @v_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
  END;;

DROP PROCEDURE IF EXISTS dbms_control.shrink_database;;
CREATE PROCEDURE dbms_control.shrink_database(v_schema_name varchar(64))
  BEGIN
    DECLARE v_table_name varchar(64);
    DECLARE done INT DEFAULT 0;
    DECLARE cursor_end CONDITION FOR SQLSTATE '02000';
    DECLARE tables_cur CURSOR FOR SELECT table_name FROM information_schema.TABLES where TABLE_SCHEMA = v_schema_name;
    DECLARE CONTINUE HANDLER FOR cursor_end SET done = 1;

    SET done = 0;
    OPEN tables_cur;
    FETCH tables_cur INTO v_table_name;
    WHILE NOT done DO
      CALL dbms_control.shrink_table(v_schema_name, v_table_name);
      FETCH tables_cur INTO v_table_name;
    END WHILE;
  END;;
  
DELIMITER ;
