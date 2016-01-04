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
	# assuming conf as json with keys "host","user","password","port","database"
	with open(conf, 'r') as conf:
		conf = json.load(conf)
	return pg2.connect( **conf )


def copy_from_s3_to_redshift(conn, s3credentials, bucketname, keyname, table, delimiter="||", quote_char="\"", na_string=None, header_rows=0, date_format="auto", compression=None, not_run=False):

	# expects s3credentials as `dict` with keys "access_key_id" and "secret_access_key" defined
	# e.g. {"access_key_id":"ASDFHAS", "secret_access_key":"ASBDSKJA"}

	sql = """COPY %(table)s FROM 's3://%(file_location)s' CREDENTIALS 'aws_access_key_id=%(access)s;aws_secret_access_key=%(secret)s' """

	data = {
		"access": AsIs(s3credentials["access_key_id"]),
		"secret": AsIs(s3credentials["secret_access_key"]),
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
		if compression.upper() in ("GZIP","LZOP"):
			sql += compression.upper() +" "

	cur = conn.cursor()
	if not_run:
		return cur.mogrify(sql, data)
	else:
		cur.execute(sql, data)
		conn.commit()
		return True

def redshift_delete_by_keys(conn, table, date_column="date", start_date=None, end_date=None, date_format="%Y-%m-%d", window=30, not_run=False):
	"""method to delete old entries from redshift by date, dates are inclusive, default span is 31 days ago to 1 day ago"""

	if end_date is None:
		end_date = ( datetime.now() - timedelta( days=1 ) ).strftime(date_format)

	if start_date is None:
		start_date = ( datetime.strptime(end_date, date_format) - timedelta( days=window ) ).strftime(date_format)

	sql = """DELETE FROM %(table)s WHERE %(date_column)s BETWEEN %(start_date)s AND %(end_date)s"""
	data = {
		"table": AsIs(table),
		"date_column": AsIs(date_column),
		"start_date": start_date,
		"end_date": end_date,
	}

	cur = conn.cursor()
	if not_run:
		return cur.mogrify(sql, data)
	else:
		cur.execute(sql, data)
		conn.commit()
		return True

def redshift_upsert(conn, s3conf, bucketname, keyname, table="placement", delimiter="\t", quote_char="\"", na_string="NA"):

	update_table = table+"_updates"

	sql = """DROP TABLE IF EXISTS %(update_table)s; CREATE TEMP TABLE %(update_table)s (LIKE %(table)s);"""
	data = {"table": AsIs(table),"update_table": AsIs(update_table) }

	cur = conn.cursor()
	cur.execute( sql, data )

	copy_from_s3_to_redshift( conn=conn, s3conf=s3conf, bucketname=bucketname, keyname=keyname, table=update_table, delimiter=delimiter, quote_char=quote_char, na_string=na_string)

	sql = """
	begin transaction;
	DELETE FROM %(table)s
	USING %(update_table)s
	WHERE %(table)s.id = %(update_table)s.id;

	INSERT INTO %(table)s (
		SELECT * FROM %(update_table)s
		);
	end transaction;
	DROP TABLE %(update_table)s;
	"""
	cur.execute(sql, data)
