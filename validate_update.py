#!/usr/bin/env python
import os
import subprocess
import argparse
import ConfigParser

root_path = os.path.abspath('./')
liqubase_cmd_path = root_path + '/database/liquibase/'
files_path = root_path + '/database/structure/'


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-a', '--action',
                        required=False,
                        action='store',
                        help='validate or update')

    parser.add_argument('--last_only',
                        required=False,
                        action='store_true',
                        help='last_only')

    parser.add_argument('--env',
                        required=False,
                        action='store',
                        help='environment')

    args = parser.parse_args()
    return args


def load_config(filepath, env):
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.readfp(filepath)
    return config.items(env)


def make_jdbc_url(params, dbname):
    return 'jdbc:mariadb://' + params['host'] + ':' + params['port'] + '/' + dbname


def liquibase_cmd(params, dbname, changelog_file, db_action):
    output = ''
    java_classpath = ''
    for file_ in os.listdir(liqubase_cmd_path):
        if file_.endswith('.jar'):
            java_classpath += file_ + ';'

    liquibase = [liqubase_cmd_path,
                 'java',
                 '-cp',
                 java_classpath,
                 os.environ["$JAVA_OPTS"],
                 'liquibase.integration.commandline.Main',
                 '--driver=' + params['driver'],
                 '--classpath=' + params['classpath'],
                 '--url=' + make_jdbc_url(params, dbname),
                 '--username=' + params['username'],
                 '--password=' + params['password'],
                 '--changeLogFile=' + changelog_file,
                 db_action
                 ]
    print(db_action + " file: " + changelog_file)

    try:
        proc = subprocess.Popen(liquibase, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def prepare_file_list(params, dbname, last_only, file_list):
    if last_only:

        query = "select DISTINCT substring_index(FileName, '/', -1) from "
        query += dbname
        query += ".DATABASECHANGELOG where lower(substring_index(substring_index(FileName, '/', -1), '.', -1)) = 'xml'"

        mysql_cmd = ['mysql',
                     '-sN',
                     '-u' + params['username'],
                     '-p' + params['password'],
                     '-h' + params['host'],
                     '-P' + params['port'],
                     '-e"' + query + '"']
        executed_files_list = subprocess.check_output(mysql_cmd)

        return list(set(file_list) - set(executed_files_list))
    else:
        return file_list


def main():
    args = get_args()

    in_db_action = args.action
    env = args.env

    props = load_config(liqubase_cmd_path + 'liquibase.properties', env)

    try:
        in_last_only = True if props[env]['last_only'] is None else False
    except ConfigParser.NoOptionError:
        in_last_only = False

    for root, dirs, files in os.walk(files_path):
        for dirname in dirs:
            data_files_list = os.listdir(os.path.join(root, dirname))
            result_list = prepare_file_list(props, dirname, in_last_only, data_files_list)

            for script in result_list:
                if script.endswith('.xml'):
                    output, err_output = liquibase_cmd(props,
                                                       dirname,
                                                       os.path.join(root, dirname, script),
                                                       in_db_action)

                    print err_output if err_output != '' else output

                    if in_db_action == 'validate' \
                            and not output.splitlines()[0].startswith('No validation errors found'):
                        exit(1)
                    if in_db_action == 'update' and err_output:
                        exit(1)


if __name__ == "__main__":
    main()


