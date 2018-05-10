#!/usr/local/bin/python3

# standard lib imports
import os
import sys
import json
import re
import logging
from datetime import datetime, timedelta

# third party lib imports
import pandas as pd
import psycopg2 as pg2
from psycopg2.extensions import AsIs


def redshift_connection(conf):
    # assuming conf as json file or dict with keys 'host','user','password','port','database'
    if type(conf) == str:
        with open(conf, 'r') as conf:
            conf = json.load(conf)
    elif type(conf) == dict:
        pass
    else:
        raise ValueError("conf must be the path to a file with valid JSON or a python dict object containing the keys [ 'host','user','password','port','database' ]")
    return pg2.connect(**conf)


def select(conn, sql, to_df=True, header=True, max_rows=100000):

    """Executes SELECT statements, and outputs to nested list or DataFrame"""

    # lowercase the SQL statement to make parsing easier. This does not affect execution in Redshift.
    sql = sql.lower()

    # ensure 'sql' is a SELECT statement
    if sql.split(" ")[0] != 'select':
        raise ValueError("'sql' argument must start with a SELECT statement.")

    # ensure there are no other operations except SELECT queries
    illegal_ops_re = re.compile(r'(delete|update|drop|insert)')
    illegal_ops_match = illegal_ops_re.search(sql)
    if illegal_ops_match:
        raise ValueError("'sql' contains illegal operation: {}. This function can only execute SELECT queries.".format(illegal_ops_match.group(0).upper()))

    # get table name and column names from 'sql'
    table_re = re.compile(r'(?<=from )[A-Za-z0-9\_]+(?=^|\s|\;|$)')
    column_re = re.compile(r'(?<=select )[A-Za-z0-9\s\_*\\(\),]+(?= from)')

    tmatch = table_re.search(sql)
    if tmatch:
        table = tmatch.group(0)
    else:
        raise ValueError("No table name found in 'sql' argument")

    cmatch = column_re.search(sql)
    columns = []
    if cmatch:
        csplit = cmatch.group(0).split(',')
        for col in csplit:
            is_wildcard = col.replace(' ', '') == "*"
            # if there's a wildcard in the columns, query pg_table_def to get column names and order
            if is_wildcard:
                cur = conn.cursor()
                cur.execute("""SELECT "column" FROM pg_table_def WHERE tablename=%(table)s""", {"table": table})
                columns += [x[0] for x in cur.fetchall()]
                cur.close()
            else:
                # if columns are renamed in query using the 'AS' operation, use the string given after 'AS'.
                # Otherwise, use column name as is
                as_split = col.split(' as ')
                if len(as_split) > 1:  # this can only be greater than 1 if there is an 'AS' operation
                    columns.append(as_split[-1].replace(' ', ''))
                else:
                    columns.append(col.replace(' ', ''))

    cur = conn.cursor()
    cur.execute(sql)
    if cur.rowcount <= max_rows:

        data = [list(x) for x in cur.fetchall()]

        if not to_df:  # return as list of lists
            if header:
                data = columns + data
            return data
        else:  # return as pandas DataFrame
            if header:
                df = pd.DataFrame(data, columns=columns)
            else:
                df = pd.DataFrame(data)
            return(df)
    else:
        raise ValueError('Query resulted in too many rows of data to output: {}. Increase max_rows parameter ({}) and try again.'.format(cur.rowcount, max_rows))
    cur.close()
    return None


def copy_from_s3(conn, s3conf, bucketname, keyname, table, delimiter=",", quote_char="\"", escape=False, na_string=None, header_rows=0, date_format="auto", compression=None, explicit_ids=False, manifest=False, not_run=False):

    if isinstance(s3conf, str):
        with open(s3conf) as fi:
            s3conf = json.load(fi)

    # expects s3conf as `dict` with keys "aws_access_key_id" and "aws_secret_access_key" defined
    # e.g. {"aws_access_key_id":"ASDFHAS", "aws_secret_access_key":"ASBDSKJA"}

    sql = """COPY %(table)s FROM 's3://%(file_location)s' CREDENTIALS 'aws_access_key_id=%(access)s;aws_secret_access_key=%(secret)s' """

    data = {
        "access": AsIs(s3conf["aws_access_key_id"]),
        "secret": AsIs(s3conf["aws_secret_access_key"]),
        "table": AsIs(table),
        "file_location": AsIs(os.path.join(bucketname, keyname))
    }

    if na_string is not None:
        sql += "NULL %(na_string)s "
        data["na_string"] = na_string

    if delimiter is not None:
        sql += "DELIMITER %(delimiter)s "
        data["delimiter"] = delimiter

    if escape:
        sql += "ESCAPE "

    if header_rows > 0:
        sql += "IGNOREHEADER %(header_rows)s "
        data["header_rows"] = header_rows

    if date_format is not None:
        sql += "DATEFORMAT %(date_format)s "
        data["date_format"] = date_format

    if quote_char is not None:
        sql += "CSV QUOTE %(quote_char)s "
        data["quote_char"] = quote_char

    if manifest:
        sql += "MANIFEST "

    if compression is not None:
        if compression.upper() in set(("GZIP", "LZOP")):
            sql += compression.upper() + " "

    if explicit_ids:
        sql += "EXPLICIT_IDS "

    cur = conn.cursor()
    if not_run:
        return cur.mogrify(sql, data)
    else:
        cur.execute(sql, data)
        conn.commit()
        return True


def unload_into_s3(conn, s3conf, bucketname, keyname, table=None, select_statement=None, select_data={}, delimiter=",", escape=False, na_string=None, compression=None, allow_overwrite=False, parallel=True, not_run=False, manifest=False):

    cur = conn.cursor()

    # one of table or select_statement must be defined. select_statement overrides table. If only table is given, select_statement is defined as 'select * from <table>'
    if table is None and select_statement is None:
        raise ValueError('One of \'table\' or \'select_statement\' arguments must be given.')
    elif table is not None and select_statement is None:
        select_statement = 'SELECT * FROM ' + table

    select_statement = str(cur.mogrify(select_statement, select_data), 'utf-8')

    if isinstance(s3conf, str):
        with open(s3conf) as fi:
            s3conf = json.load(fi)

    # expects s3conf as `dict` with keys "aws_access_key_id" and "aws_secret_access_key" defined
    # e.g. {"aws_access_key_id":"ASDFHAS", "aws_secret_access_key":"ASBDSKJA"}

    sql = "UNLOAD (%(select_statement)s) TO 's3://%(file_location)s' CREDENTIALS 'aws_access_key_id=%(access)s;aws_secret_access_key=%(secret)s' "

    data = {
        "access": AsIs(s3conf["aws_access_key_id"]),
        "secret": AsIs(s3conf["aws_secret_access_key"]),
        "select_statement": select_statement,
        "file_location": AsIs(os.path.join(bucketname, keyname))
    }

    if na_string is not None:
        sql += "NULL %(na_string)s "
        data["na_string"] = na_string

    if delimiter is not None:
        sql += "DELIMITER %(delimiter)s "
        data["delimiter"] = delimiter

    if compression is not None:
        if compression.upper() in set(("GZIP", "LZOP")):
            sql += compression.upper() + " "

    if escape:
        sql += "ESCAPE "

    if allow_overwrite:
        sql += "ALLOWOVERWRITE "

    if manifest:
        sql += "MANIFEST "

    if parallel:
        sql += "PARALLEL ON "
    else:
        sql += "PARALLEL OFF "

    if not_run:
        return cur.mogrify(sql, data)
    else:
        cur.execute(sql, data)
        conn.commit()
        return True


def delete(conn, table, not_run=False, params={}):
    """method to delete data from table. Params are 'AND'ed """

    data = {
        "table": AsIs(table),
    }

    sql = """DELETE FROM %(table)s"""
    where = []
    for param in params:
        data[param + "_col"] = AsIs(param)
        if isinstance(params[param], str):
            where.append("%({}_col)s = %({}_val)s".format(param, param))
            data[param + "_val"] = params[param]
        elif isinstance(params[param], list):
            where.append("%({}_col)s in (%({}_val)s)".format(param, param))
            data[param + "_val"] = AsIs(','.join(["'{}'".format(p) for p in params[param]]))

    if len(where) > 0:
        sql += " WHERE " + " AND ".join(where)

    cur = conn.cursor()
    if not_run:
        return cur.mogrify(sql, data)
    else:
        cur.execute(sql, data)
        conn.commit()
        return True


def delete_by_date(conn, table, date_column="date", start_date=None, end_date=None, date_format="%Y-%m-%d", window=30, not_run=False, params={}):
    """method to delete old entries from redshift by date, dates are inclusive, default span is 31 days ago to 1 day ago"""

    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime(date_format)

    if start_date is None:
        start_date = (datetime.strptime(end_date, date_format) - timedelta(days=window)).strftime(date_format)

    data = {
        "table": AsIs(table),
        "date_column": AsIs(date_column),
        "start_date": start_date,
        "end_date": end_date,
    }

    sql = """DELETE FROM %(table)s WHERE """
    where = ["%(date_column)s BETWEEN %(start_date)s AND %(end_date)s"]

    for param in params:
        data[param + "_col"] = AsIs(param)
        if isinstance(params[param], str):
            where.append("%({}_col)s = %({}_val)s".format(param, param))
            data[param + "_val"] = params[param]
        elif isinstance(params[param], list):
            where.append("%({}_col)s in (%({}_val)s)".format(param, param))
            data[param + "_val"] = AsIs(','.join(["'{}'".format(p) for p in params[param]]))

    sql += " AND ".join(where)

    cur = conn.cursor()
    if not_run:
        return cur.mogrify(sql, data)
    else:
        cur.execute(sql, data)
        conn.commit()
        return True


def upsert(conn, s3conf, bucketname, keyname, table, unique_key="id", delimiter="\t", quote_char="\"", na_string="NA", compression=None):

    update_table = table + "_updates"

    # create temp table in Redshift
    data = {
        "table": AsIs(table),
        "update_table": AsIs(update_table),
        "unique_key": AsIs(unique_key)}

    sql = """DROP TABLE IF EXISTS %(update_table)s; CREATE TEMP TABLE %(update_table)s (LIKE %(table)s);"""

    cur = conn.cursor()
    cur.execute(sql, data)

    # insert data into new temp table
    copy_from_s3(conn=conn, s3conf=s3conf, bucketname=bucketname, keyname=keyname, table=update_table, delimiter=delimiter, quote_char=quote_char, na_string=na_string, compression=compression)

    # upsert data from temp table into original table, drop temp table
    sql = """
    begin transaction;
    DELETE FROM %(table)s
    USING %(update_table)s
    WHERE %(table)s.%(unique_key)s = %(update_table)s.%(unique_key)s;

    INSERT INTO %(table)s (
        SELECT * FROM %(update_table)s
        );
    end transaction;
    DROP TABLE %(update_table)s;
    """
    cur.execute(sql, data)


def vacuum_analyze(conn):
    logging.info("Running Vacuum/Analyze on Redshift")
    iso_lvl = conn.isolation_level
    conn.set_isolation_level(0)  # isolation to 0 allows queries like vacuum which do not occur within a transaction block
    rcur = conn.cursor()
    rcur.execute("vacuum;analyze;")
    conn.commit()
    conn.set_isolation_level(iso_lvl)  # reset to original setting
