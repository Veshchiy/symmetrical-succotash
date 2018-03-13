#!/usr/bin/env python
import os
import subprocess
import argparse
import ConfigParser

from datetime import date

root_path = os.path.abspath('./')
liqubase_cmd_path = root_path + '/database/liquibase/'
files_path = root_path + '/database/structure/'


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--env',
                        required=False,
                        action='store',
                        help='environment')

    parser.add_argument('--ref_env',
                        required=False,
                        action='store',
                        help='referenced environment')

    args = parser.parse_args()
    return args


def load_config(filepath, env):
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.readfp(filepath)
    return config.items(env)


def make_jdbc_url(params, dbname):
    return 'jdbc:mariadb://' + params['host'] + ':' + params['port'] + '/' + dbname


def liquibase_cmd(params, dbname, changelog_file, db_action, ret_filename=''):
    output = ''
    java_classpath = ''
    for file_ in os.listdir(liqubase_cmd_path):
        if file_.endswith('.jar'):
            java_classpath += file_ + ';'

    jdbc_url = 'jdbc:mariadb://' + params['host'] + ':' + params['port'] + '/' + dbname

    liquibase = [liqubase_cmd_path,
                 'java',
                 '-cp',
                 java_classpath,
                 os.environ["$JAVA_OPTS"],
                 'liquibase.integration.commandline.Main',
                 '--driver=' + params['driver'],
                 '--classpath=' + params['classpath'],
                 '--url=' + jdbc_url,
                 '--username=' + params['username'],
                 '--password=' + params['password'],
                 '--changeLogFile=' + changelog_file,
                 db_action,
                 ' > ' + ret_filename if ret_filename != '' else ''
                 ]
    print(db_action + " file: " + changelog_file)

    try:
        proc = subprocess.Popen(liquibase, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def liquibase_cmd_diff(cur_params, ref_params, dbname, ret_filename=''):
    output = ''
    java_classpath = ''
    for file_ in os.listdir(liqubase_cmd_path):
        if file_.endswith('.jar'):
            java_classpath += file_ + ';'

    if cur_params['driver'] != ref_params['driver']:
        raise Exception("Can`t make diff between different types of DBMS!!!")
    else:
        liquibase = [liqubase_cmd_path,
                     'java',
                     '-cp',
                     java_classpath,
                     os.environ["$JAVA_OPTS"],
                     'liquibase.integration.commandline.Main',
                     '--driver=' + cur_params['driver'],
                     '--classpath=' + cur_params['classpath'],
                     '--url=' + make_jdbc_url(cur_params, dbname),
                     '--username=' + cur_params['username'],
                     '--password=' + cur_params['password'],
                     'diffChangeLog',
                     '--referenceUrl=' + make_jdbc_url(ref_params, dbname),
                     '--referenceUsername=' + ref_params['username'],
                     '--referencePassword=' + ref_params['password'],
                     ' > ' + ret_filename if ret_filename != '' else ''
                     ]

        try:
            proc = subprocess.Popen(liquibase, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, err_output = proc.communicate()
        except subprocess.CalledProcessError as e:
            err_output = e.output

        return output, err_output


def main():
    args = get_args()

    in_db_action = args.action
    env = args.env
    ref_env = args.ref_env
    in_dbname = args.db_name
    out_file = args.out_file

    cur_params = load_config(liqubase_cmd_path + 'liquibase.properties', env)
    ref_params = load_config(liqubase_cmd_path + 'liquibase.properties', ref_env)

    if os.path.isdir(out_file):
        changelog_fname = env + '_' + ref_env + '_' + in_dbname + '_' + date.today().strftime('%Y%m%d')
        xml_out_path = os.path.join(out_file, changelog_fname.join('.xml'))
        sql_out_path = os.path.join(out_file, changelog_fname.join('.sql'))
    elif os.path.exists(out_file):
        fpath = os.path.split(out_file)[0]
        fname = os.path.split(out_file)[1]
        changelog_fname = fname.rpartition('.')[0]
        xml_out_path = os.path.join(fpath, changelog_fname.join('.xml'))
        sql_out_path = os.path.join(fpath, changelog_fname.join('.sql'))
        os.remove(out_file)
    else:
        fpath = os.path.split(out_file)[0]
        fname = os.path.split(out_file)[1]
        changelog_fname = fname.rpartition('.')[0]
        xml_out_path = os.path.join(fpath, changelog_fname.join('.xml'))
        sql_out_path = os.path.join(fpath, changelog_fname.join('.sql'))

    output, err_output =  liquibase_cmd_diff(cur_params, ref_params, in_dbname, xml_out_path)

    print err_output if err_output != '' else output

    output, err_output = liquibase_cmd(ref_params,
                                       in_dbname,
                                       xml_out_path,
                                       in_db_action,
                                       sql_out_path)
    os.remove(xml_out_path)

    print err_output if err_output != '' else output

    if (in_db_action == 'validate'
            and not output.splitlines()[0].startswith('No validation errors found'))\
            or (in_db_action == 'update' and err_output):
        exit(1)


if __name__ == "__main__":
    main()


