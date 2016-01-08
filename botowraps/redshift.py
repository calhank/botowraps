#!/usr/local/bin/python3

# standard lib imports
import os
import sys
import json
import logging
from datetime import datetime, timedelta

# third party lib imports
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
	return pg2.connect( **conf )


def copy_from_s3(conn, s3conf, bucketname, keyname, table, delimiter="||", quote_char="\"", na_string=None, header_rows=0, date_format="auto", compression=None, not_run=False):

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

	if header_rows > 0:
		sql += "IGNOREHEADER %(header_rows)s "
		data["header_rows"] = header_rows

	if date_format is not None:
		sql += "DATEFORMAT %(date_format)s "
		data["date_format"] = date_format

	if quote_char is not None:
		sql += "CSV QUOTE %(quote_char)s "
		data["quote_char"] = quote_char

	if compression is not None:
		if compression.upper() in set(("GZIP","LZOP")):
			sql += compression.upper() +" "

	cur = conn.cursor()
	if not_run:
		return cur.mogrify(sql, data)
	else:
		cur.execute(sql, data)
		conn.commit()
		return True

def delete_by_date(conn, table, date_column="date", start_date=None, end_date=None, date_format="%Y-%m-%d", window=30, not_run=False, params={} ):
	"""method to delete old entries from redshift by date, dates are inclusive, default span is 31 days ago to 1 day ago"""

	if end_date is None:
		end_date = ( datetime.now() - timedelta( days=1 ) ).strftime(date_format)

	if start_date is None:
		start_date = ( datetime.strptime(end_date, date_format) - timedelta( days=window ) ).strftime(date_format)

	data = {
		"table": AsIs(table),
		"date_column": AsIs(date_column),
		"start_date": start_date,
		"end_date": end_date,
	}
	
	sql = """DELETE FROM %(table)s WHERE """
	where = ["%(date_column)s BETWEEN %(start_date)s AND %(end_date)s"]

	for param in params:
		where.append("%%({p}_col)s = %%({p}_val)s".format(param))
		data[param+"_col"] = AsIs(param)
		data[param+"_val"] = params[param]

	sql += " AND ".join(where)

	cur = conn.cursor()
	if not_run:
		return cur.mogrify(sql, data)
	else:
		cur.execute(sql, data)
		conn.commit()
		return True

def upsert(conn, s3conf, bucketname, keyname, table, unique_key="id", delimiter="\t", quote_char="\"", na_string="NA", compression=None):

	update_table = table+"_updates"

	# create temp table in Redshift
	data = {
		"table": AsIs(table),
		"update_table": AsIs(update_table),
		"unique_key": AsIs(unique_key) }
	
	sql = """DROP TABLE IF EXISTS %(update_table)s; CREATE TEMP TABLE %(update_table)s (LIKE %(table)s);"""

	cur = conn.cursor()
	cur.execute( sql, data )

	# insert data into new temp table
	copy_from_s3( conn=conn, s3conf=s3conf, bucketname=bucketname, keyname=keyname, table=update_table, delimiter=delimiter, quote_char=quote_char, na_string=na_string, compression=compression)

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
	cur.execute( sql, data )

