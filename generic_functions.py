import collections
import csv
import datetime
import logging
import os
import random
import shlex
import shutil
import smtplib
import subprocess
import time
import zipfile
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from os import listdir
from os.path import isfile, join
from subprocess import PIPE, STDOUT, Popen

import pandas as pd

from pandora.util.sample.connection import (get_boto_client, get_postgres_con,
                                            get_redshift_con, get_value)


"""Generic functions like cleanup s3 ,send mail ,finding previous quarter 
    Can Easily port across the projects ,  Teams with minimal change
"""


def redshift_clean(v_table_name, v_schema_name, src_db='BI'):

    size_qry = """
    select trim(pgdb.datname) as Database, trim(pgn.nspname) as Schema,
    trim(a.name) as Table, b.mbytes, a.rows
    from ( select db_id, id, name, sum(rows) as rows from stv_tbl_perm a group by db_id, id, name ) as a
    join pg_class as pgc on pgc.oid = a.id
    join pg_namespace as pgn on pgn.oid = pgc.relnamespace
    join pg_database as pgdb on pgdb.oid = a.db_id
    join (select tbl, count(*) as mbytes
    from stv_blocklist group by tbl) b on a.id=b.tbl
    where trim(pgn.nspname)='{schema}' and  trim(a.name) ='{tab_name}'
    """.format(schema=v_schema_name, tab_name=v_table_name)

    print size_qry

    con_bi = get_redshift_con(src_db)

    cur_bi = con_bi.cursor()

    cur_bi.execute(size_qry)

    table_val = cur_bi.fetchone()

    print ' Table {schema}.{tab_name} size is {table_size} MB row count:{row_cnt} Date:{date}'.format(schema=v_schema_name,
                                                                                                      tab_name=v_table_name, table_size=table_val[3], row_cnt=table_val[4], date=datetime.datetime.now())

    con_bi.close()

    crt_sql = " create table {schema}.{tab_name}_bkp as select * from {schema}.{tab_name} ".format(
        schema=v_schema_name, tab_name=v_table_name)
    trt_sql = " truncate table {schema}.{tab_name} ".format(
        schema=v_schema_name, tab_name=v_table_name)
    ins_sql = " insert into  {schema}.{tab_name}  select * from {schema}.{tab_name}_bkp".format(
        schema=v_schema_name, tab_name=v_table_name)
    drp_sql = " drop table {schema}.{tab_name}_bkp".format(
        schema=v_schema_name, tab_name=v_table_name)

    con_bi = get_redshift_con(src_db)

    cur_bi = con_bi.cursor()
    print crt_sql
    cur_bi.execute(crt_sql)
    print trt_sql
    cur_bi.execute(trt_sql)
    print ins_sql
    cur_bi.execute(ins_sql)
    print drp_sql
    cur_bi.execute(drp_sql)
    con_bi.commit()
    con_bi.close()

    con_bi = get_redshift_con(src_db)

    cur_bi = con_bi.cursor()

    cur_bi.execute(size_qry)

    table_val = cur_bi.fetchone()

    print ' Table {schema}.{tab_name} size is {table_size} MB row count:{row_cnt} Date:{date}'.format(schema=v_schema_name,
                                                                                                      tab_name=v_table_name, table_size=table_val[3], row_cnt=table_val[4], date=datetime.datetime.now())
    con_bi.close()


def postgres_clean(v_table_name, v_schema_name, src_db='ODS'):

    size_qry = """
        SELECT schema_name, relname as table_name,
               pg_size_pretty(sum(table_size)::bigint) table_size
        FROM (
          SELECT relname,pg_catalog.pg_namespace.nspname as schema_name,
                 pg_relation_size(pg_catalog.pg_class.oid) as table_size
          FROM   pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
        ) t
        where schema_name='{schema}' and relname='{tab_name}'
        GROUP BY schema_name,relname
        ORDER BY schema_name
    """.format(schema=v_schema_name, tab_name=v_table_name)

    print size_qry

    con_bi = get_postgres_con(src_db)

    cur_bi = con_bi.cursor()

    cur_bi.execute(size_qry)

    table_val = cur_bi.fetchone()

    print ' Table {schema}.{tab_name} size is {table_size}   Date:{date}'.format(schema=v_schema_name,
                                                                                 tab_name=v_table_name, table_size=table_val[2], date=datetime.datetime.now())

    con_bi.close()

    crt_sql = " create table {schema}.{tab_name}_bkp as select * from {schema}.{tab_name} ".format(
        schema=v_schema_name, tab_name=v_table_name)
    trt_sql = " truncate table {schema}.{tab_name} ".format(
        schema=v_schema_name, tab_name=v_table_name)
    ins_sql = " insert into  {schema}.{tab_name}  select * from {schema}.{tab_name}_bkp".format(
        schema=v_schema_name, tab_name=v_table_name)
    drp_sql = " drop table {schema}.{tab_name}_bkp".format(
        schema=v_schema_name, tab_name=v_table_name)

    con_bi = get_postgres_con(src_db)

    cur_bi = con_bi.cursor()
    print crt_sql
    cur_bi.execute(crt_sql)
    print trt_sql
    cur_bi.execute(trt_sql)
    print ins_sql
    cur_bi.execute(ins_sql)
    print drp_sql
    cur_bi.execute(drp_sql)
    con_bi.commit()
    con_bi.close()

    con_bi = get_postgres_con(src_db)

    cur_bi = con_bi.cursor()

    cur_bi.execute(size_qry)

    table_val = cur_bi.fetchone()

    print ' Table {schema}.{tab_name} size is {table_size}  Date:{date}'.format(schema=v_schema_name,
                                                                                tab_name=v_table_name, table_size=table_val[2], date=datetime.datetime.now())
    con_bi.close()


def redshift_s3_unload(v_table_name, v_schema_name, src_db='BI', delim="|"):

    sel_sql = " select * from {schema}.{tab_name} ".format(
        schema=v_schema_name, tab_name=v_table_name)
    s3_file_name = "s3://bi.mylocation.com/prod/redshift_tables/{schema}/{file_name}".format(
        schema=v_schema_name, file_name=v_table_name)

    unload_sql = " unload ('{sel_sql}')" \
                 " to '{s3_file_name}' credentials " \
                 " 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' " \
                 " delimiter as '{delim}'  parallel off " \
                 "addquotes escape allowoverwrite; ".format(sel_sql=sel_sql, s3_file_name=s3_file_name,
                                                            access_key=os.environ['AWS_ACCESS_KEY_ID'],
                                                            secret_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                                            delim=delim)

    print unload_sql

    con_bi = get_redshift_con(src_db)

    cur_bi = con_bi.cursor()

    cur_bi.execute(unload_sql)

    con_bi.close()


def redshift_s3_load(v_table_name, v_schema_name, src_db='BI', delim="|"):

    s3_file_name = "s3://bi.mylocation.com/prod/redshift_tables/{schema}/{file_name}".format(schema=v_schema_name,
                                                                                          file_name=v_table_name)

    copy_cmd = "copy {schema}.{tab_name}) " \
               "from '{s3_file_name}' credentials" \
               "'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'" \
               "delimiter '{delim}'  removequotes escape;".format(schema=v_schema_name, tab_name=v_table_name,
                                                                  s3_file_name=s3_file_name,
                                                                  access_key=os.environ['AWS_ACCESS_KEY_ID'],
                                                                  secret_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                                                  delim=delim)
    print copy_cmd

    con_mar = get_redshift_con(src_db)
    cur_mar = con_mar.cursor()

    cur_mar.execute(copy_cmd)

    con_mar.commit()
    con_mar.close()


def postgres_data_load(conn, table_name, file_name, trunc_flag='N', delim=','):
    SQL_STATEMENT = """
        COPY {tab_name} FROM STDIN WITH
            CSV
            HEADER
            NULL ''
            QUOTE '"'
            DELIMITER AS '{delim}'
        """.format(tab_name=table_name, delim=delim)
    my_file = open(file_name)

    cursor = conn.cursor()
    if trunc_flag == 'Y':
        print " Truncating table before load "+table_name
        cursor.execute(" truncate table "+table_name)

    cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
    conn.commit()
    cursor.close()


def postgres_data_unload(conn, query, filename, delim=','):
    SQL_STATEMENT = """
        COPY ({query}) to STDOUT    
            CSV
            HEADER
            NULL ''
            QUOTE '"'
            DELIMITER AS '{delim}' """.format(query=query, delim=delim)

    cursor = conn.cursor()
    print SQL_STATEMENT
    file = open(filename, 'w')
    cursor.copy_expert(SQL_STATEMENT, file)
    file.close()
    conn.commit()
    cursor.close()


def uniqueid():
    return random.getrandbits(32)


def log_subprocess_output(pipe):
    for line in iter(pipe.readline, b''):  # b'\n'-separated lines
        logging.info('got line from subprocess: %r', line)


def shell_cmd(cmd):
    print " executing " + cmd

    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout)
    exitcode = process.wait()
    return exitcode


def list_local_folder(fullpath):

    onlyfiles = [f for f in listdir(fullpath) if isfile(join(fullpath, f))]
    return onlyfiles


def check_file_s3(source_bucket, source_key, credentials={}):

    s3conn = get_boto_client(credentials)

    S3Bucket = source_bucket

    childs = s3conn.list_objects(Bucket=S3Bucket, Prefix=source_key)

    if 'Contents' in childs:

        if len(childs["Contents"]) > 0:

            print 'files/logs found in : bucket {}/{}'.format(
                source_bucket, source_key)

            return 1

        else:
            print 'No files/logs found in : bucket {}/{}'.format(
                source_bucket, source_key)
            return 0

    else:
        print 'No files/logs found in : bucket {}/{}'.format(
            source_bucket, source_key)
        return 0


def move_file_s3(source_bucket, source_key, dest_bucket, dest_key, type='SINGLE_MOVE', credentials={}):

    s3conn = get_boto_client(credentials)

    S3Bucket = source_bucket

    childs = s3conn.list_objects(Bucket=S3Bucket, Prefix=source_key)

    print childs

    s3conn_copy = get_boto_client(credentials)

    if 'Contents' in childs:

        if type.split('_')[0] == 'SINGLE' and len(childs["Contents"]) == 1:

            copy_source = {
                'Bucket': source_bucket,
                'Key': childs["Contents"][0]["Key"]
            }

            print " File copying to  {}/{}".format(dest_bucket, dest_key)
            s3conn_copy.copy(copy_source, dest_bucket, dest_key)

            if type.split('_')[1] == 'MOVE':
                print 'removing file from dir: {}'.format(source_key)
                for i in childs["Contents"]:
                    print i['Key']
                    s3conn_del = get_boto_client(credentials)
                    s3conn_del.delete_object(Bucket=S3Bucket, Key=i['Key'])

                print 'File moved sucessfully'
            else:
                print 'File copied sucessfully'

            return 1

        elif type == 'MULTI_MOVE':

            s3conn_del = get_boto_client(credentials)

            no_files = 0

            for i in childs["Contents"]:
                no_files = no_files+1
                try:
                    if not i["Key"].endswith('/'):

                        copy_source = {
                            'Bucket': source_bucket,
                            'Key': i["Key"]
                        }

                        dest_file = "/".join(dest_key.split("/")
                                             [:-1])+'/'+i["Key"].split('/')[-1]

                        source_file = i["Key"]

                        print " File copying to  {}/{}".format(
                            dest_bucket, dest_file)
                        s3conn_copy.copy(copy_source, dest_bucket, dest_file)

                        if no_files % 75 == 0:
                            del s3conn_copy
                            del s3conn_del
                            s3conn_copy = get_boto_client(credentials)
                            s3conn_del = get_boto_client(credentials)

                        print 'removing file from dir: {}'.format(source_file)

                        s3conn_del.delete_object(Bucket=S3Bucket, Key=i['Key'])

                        print 'File moved sucessfully'
                except:
                    print ' file not found {} proceeding to next file '.format(str(source_file))

            return no_files

        elif type == 'MULTI_COPY':

            no_files = 0

            for i in childs["Contents"]:
                no_files = no_files + 1
                if not i["Key"].endswith('/'):

                    copy_source = {
                        'Bucket': source_bucket,
                        'Key': i["Key"]
                    }

                    dest_file = "/".join(dest_key.split("/")
                                         [:-1]) + '/' + i["Key"].split('/')[-1]

                    print " File copying to  {}/{}".format(
                        dest_bucket, dest_file)
                    s3conn_copy.copy(copy_source, dest_bucket, dest_file)

                    if no_files % 75 == 0:
                        s3conn_copy = get_boto_client(credentials)

                    print 'File Copied sucessfully'

            return no_files

        else:
            print " Folder copy/delete is not allowed in this mode :Contact Support "
            return 0

    else:
        print 'No files/logs found in : bucket {}/{}'.format(
            source_bucket, source_key)
        return 0


def s3_file_upload(source_file, dest_file, bucket=''):

    s3_client = get_boto_client()

    s3loc = dest_file
    if not bucket:
        s3_bucket = get_value('s3_bucket')
    else:
        s3_bucket = bucket

    print s3_bucket

    s3_client.upload_file(source_file, s3_bucket, s3loc)

    s3_file_name = "s3://{}/{}".format(s3_bucket, s3loc)

    print ' File uploaded to this s3 loc :' + s3_file_name

    return s3_file_name


def get_columns_redshift(v_table_name, v_dest_db='BI',reserved_columns='N'):
    con = get_redshift_con(v_dest_db)
    cur = con.cursor()
    cur.execute(
        "select * from {tab_name} where 1=2".format(tab_name=v_table_name))
    columns = list(map(lambda x: x[0], cur.description))
    con.close()
    if reserved_columns=='Y':
        column_list = '","'.join(columns)
        column_list = '"' + column_list + '"'
    else:
        column_list =','.join(columns)

    return column_list




def get_columns_postgres(v_table_name, v_dest_db='ODS'):
    con = get_postgres_con(v_dest_db)
    cur = con.cursor()
    cur.execute(
        "select * from {tab_name} where 1=2".format(tab_name=v_table_name))
    columns = list(map(lambda x: x[0], cur.description))
    con.close()

    return ','.join(columns)


def previous_quarter(ref):
    if ref.month < 4:
        return datetime.date(ref.year - 1, 9, 01)
    elif ref.month < 7:
        return datetime.date(ref.year, 01, 01)
    elif ref.month < 10:
        return datetime.date(ref.year, 04, 01)
    return datetime.date(ref.year, 07, 01)


def create_csv(filename, cursor, delim=','):
    with open(filename, "wb") as csv_file:
        csv_writer = csv.writer(csv_file, quotechar='"',
                                quoting=csv.QUOTE_NONNUMERIC, delimiter=delim)
        csv_writer.writerow([i[0]
                             for i in cursor.description])  # write headers
        csv_writer.writerows(cursor)

    return filename


def check_file_size(filename):
    b = os.path.getsize(filename)
    return b


def compress_file(input, output):

    output_zip = zipfile.ZipFile(output, 'w')
    output_zip.write(input, compress_type=zipfile.ZIP_DEFLATED)

    output_zip.close()


def sample_mailer(toaddr, subject, body, attach_file={}, host_details={}):

    if not host_details:
        smtp_host = os.environ['SMTP_HOST']
        smtp_port = os.environ['SMTP_PORT']
        smtp_username = os.environ['SMTP_USER']
        smtp_password = os.environ['SMTP_PASSWORD']

    else:
        smtp_host = host_details['SMTP_HOST']
        smtp_port = host_details['SMTP_PORT']
        smtp_username = host_details['SMTP_USER']
        smtp_password = host_details['SMTP_PASSWORD']

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)

    fromaddr = smtp_username

    msg = MIMEMultipart()
    msg['From'] = fromaddr

    msg['To'] = ", ".join(toaddr)

    # if ','  in toaddr:
    #     msg['To'] = ", ".join(toaddr)
    # else:
    #     msg['To'] =toaddr

    msg['Subject'] = subject
    if not attach_file:
        msg.attach(MIMEText(body, 'plain'))
    else:
        filename = attach_file['FILE_PATH']+attach_file['FILE_NAME']
        f = file(filename)
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition',
                              'attachment', filename=attach_file['FILE_NAME'])
        msg.attach(attachment)

    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    print ' Mail Delivered Sucessfully '
    server.quit()


def nested_dict_iter(nested):

    for key, value in nested.iteritems():

        if isinstance(value, collections.Mapping):

            for inner_key, inner_value in nested_dict_iter(value):

                yield key+'.'+inner_key, inner_value
        else:

            yield key, value


def run_command(command):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print output.strip()
    rc = process.poll()
    return rc


def file_row_count(filename):

    with open(filename) as myfile:
        count = sum(1 for line in myfile if line.rstrip('\n'))
    return count





def check_cnt(query, DB='BI'):
    if DB == 'BI':
        con = get_redshift_con(DB)
        cur = con.cursor()
        cur.execute(query)
        cnt = cur.fetchone()[0]

    if DB == 'ODS':
        con = get_postgres_con(DB)
        cur = con.cursor()
        cur.execute(query)
        cnt = cur.execute
        cnt = cur.fetchone()[0]

    con.close()
    return cnt


def remove(path):
    """
    Remove the file or directory
    """
    if os.path.isdir(path):
        try:
            print ' removing directory:'+str(path)
            os.rmdir(path)
        except OSError:
            print "Unable to remove folder: %s" % path
    else:
        try:
            if os.path.exists(path):
                print ' removing file:' + str(path)
                os.remove(path)
        except OSError:
            print "Unable to remove file: %s" % path


def cleanup(path, number_of_days=2, recurive='N'):
    """
    Removes files from the passed in path that are older than or equal
    to the number_of_days
    """
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for root, dirs, files in os.walk(path, topdown=False):
        # to avoid recursive search on child folders paths
        if recurive == 'N' and root <> path:
            continue

        for file_ in files:
            full_path = os.path.join(root, file_)
            stat = os.stat(full_path)

            if stat.st_mtime <= time_in_secs:
                remove(full_path)

        if not os.listdir(root):
            remove(root)


def strip_control_characters(input):

    if input:

        import re

        # unicode invalid characters
        RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
                         u'|' + \
                         u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
            (unichr(0xd800), unichr(0xdbff), unichr(0xdc00), unichr(0xdfff),
             unichr(0xd800), unichr(0xdbff), unichr(
                0xdc00), unichr(0xdfff),
             unichr(0xd800), unichr(0xdbff), unichr(
                0xdc00), unichr(0xdfff),
             )
        input = re.sub(RE_XML_ILLEGAL, "", input)

        # ascii control characters
        input = re.sub(r"[\x01-\x1F\x7F]", "", input)

    return input


def ignore_nonutf8(src_file, tar_file):
    if os.path.exists(tar_file):
        print ' File removed before conversion' + str(tar_file)
        os.remove(tar_file)
    with open(src_file, "r") as fp:
        with open(tar_file, "w") as tf:
            for line in fp:
                line = strip_control_characters(line)
                line = line.decode('utf-8', 'ignore').encode("utf-8")
                if not line:
                    continue
                tf.write(line+'\n')


def encode_utf8(src_file, tar_file):
    if os.path.exists(tar_file):
        print ' File removed before conversion' + str(tar_file)
        os.remove(tar_file)
    with open(src_file, "r") as fp:
        with open(tar_file, "w") as tf:
            for line in fp:
                line = line.strip()
                line = line.decode('utf-8').encode("utf-8")
                if not line:
                    continue
                tf.write(line+'\n')


def remove_emoji(text):
    import re
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)

    return emoji_pattern.sub(r'', text)


def awscli_function(source_path, dest_path='', function='ls', profile='default', recursive='no', file=''):
    """
    Method to perform S3 operations AWSCLI package
        :param source_path: The Path of the Source S3 file (e.g). s3://s3.amazon.com/test/
        :param dest_path='': The Path of the Destination S3 file (e.g). s3://s3.amazon.com/test/
        :param function='ls': The function to be performed, ls,cp and mv
        :param profile='default': The S3 credentials profile to be used for the processing
        :param recursive='no': Incase to repeatdy move/copy files then set it as yes
        :param file='': The wildcard search string for a file to be processed
    """
    import os
    from awscli.clidriver import create_clidriver
    driver = create_clidriver()
    if recursive == 'yes':
        rec = '--recursive'
    else:
        rec = ''
    if file <> '':
        if function == 'ls':
            flnm = ''
            dest_path = ''
        else:
            flnm = "--exclude '*' --include '" + file+"*'"
    else:
        flnm=''
    prfl = '--profile=' + profile
    code = 's3 ' + function + ' ' + source_path + ' ' + dest_path + ' ' + flnm + ' ' + rec + ' ' + prfl
    driver.main(str(code).split())
    return True
