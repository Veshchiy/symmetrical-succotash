import errno, os, shutil, subprocess
from pwd import getpwnam
import grp

def make_full_backup(out_dir_full):
    output = ''
    try:
        os.makedirs(out_dir_full)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    for the_file in os.listdir(out_dir_full):
        file_path = os.path.join(out_dir_full, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)

    backup = [
        'innobackupex',
        '--rsync',
        '--user=',
        '--password=',
        '--no-timestamp',
        '--compress',
        '--compress-threads=4',
        out_dir_full
    ]

    try:
        proc = subprocess.Popen(backup, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def make_inc_backup(out_dir_full):
    full_back_dir = os.path.join(out_dir_full, 'FULL')

    if not os.path.exists(full_back_dir):
        raise Exception('ERROR: no full backup has been done before. aborting')

    if not os.path.exists(os.path.join(full_back_dir, 'last_incremental_number')):
        inc_number = 1
    else:
        f = open(os.path.join(full_back_dir, 'last_incremental_number'), 'r')
        inc_number = f.readline()
        f.close()

    basedir = os.path.join(out_dir_full, 'inc' + inc_number) if inc_number != 1 else full_back_dir

    backdir = os.path.join(out_dir_full, 'inc' + str(inc_number + 1))

    backup = [
        'innobackupex',
        '--rsync',
        '--user=',
        '--password=',
        '--no-timestamp',
        '--compress',
        '--compress-threads=4',
        '--incremental',
        backdir,
        '--incremental-basedir=' + basedir
    ]

    output = ''
    try:
        proc = subprocess.Popen(backup, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    print output, err_output

    f = open(os.path.join(out_dir_full, 'last_incremental_number'), 'w')
    f.write(str(inc_number + 1))
    f.close()


def decompress_backup(out_dir, subdir):
    decompress = [
        'innobackupex',
        '--decompress',
        os.path.join(out_dir, subdir)
    ]

    output = ''
    try:
        proc = subprocess.Popen(decompress, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def restore_backup(out_dir, subdir, apply_log, redo, inc_dir = ''):
    output = ''

    restore = [
        'innobackupex',
        '--apply-log' if apply_log else '',
        '--redo-only' if redo else '',
        os.path.join(out_dir, subdir),
        '--incremental-dir=' + os.path.join(out_dir, inc_dir) if inc_dir <> '' else ''
    ]

    try:
        proc = subprocess.Popen(restore, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def copy_back_data(out_dir, subdir):
    output = ''

    restore_full = [
        'innobackupex',
        '--copy-back',
        os.path.join(out_dir, subdir),
    ]

    try:
        proc = subprocess.Popen(restore_full, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err_output = proc.communicate()
    except subprocess.CalledProcessError as e:
        err_output = e.output

    return output, err_output


def make_restore(out_dir_full):

    output, err_output = decompress_backup(out_dir_full, 'FULL')
    print output, err_output

    output, err_output =restore_backup(out_dir_full, 'FULL', True, True)
    print output, err_output

    inc_num = 1
    inc_dir = os.path.join(out_dir_full, 'inc' + str(inc_num))
    inc_dir_next = os.path.join(out_dir_full, 'inc' + str(inc_num + 1))

    while os.path.exists(inc_dir) and os.path.exists(inc_dir_next):
        inc_dir = os.path.join(out_dir_full, 'inc' + str(inc_num))
        inc_dir_next = os.path.join(out_dir_full, 'inc' + str(inc_num + 1))

        output, err_output = decompress_backup(out_dir_full, inc_dir)
        print output, err_output

        output, err_output = restore_backup(out_dir_full, inc_dir, True, True)
        print output, err_output

        inc_num =+ 1

    if os.path.exists(inc_dir):
        # the last incremental has to be applied without the redo-only flag
        output, err_output = decompress_backup(out_dir_full, inc_dir)
        print output, err_output

        output, err_output = restore_backup(out_dir_full, 'FULL', True, False, inc_dir)
        print output, err_output

    output, err_output = restore_backup(out_dir_full, 'FULL', True, False)
    print output, err_output

    shutil.copytree(data_dir, data_dir_back)
    shutil.rmtree(data_dir)

    output, err_output = copy_back_data(out_dir_full, 'FULL')
    print output, err_output

    mysql_uid = getpwnam('mysql').pw_uid
    mysql_gid = grp.getgrnam('mysql').gr_gid

    os.chown(data_dir, mysql_uid, mysql_gid)