#!/usr/local/bin/python3

# standard lib imports
import os
import io
import sys
import time
import json
import random
import logging
import multiprocessing

# third party lib imports
from boto.s3.connection import S3Connection

class S3Uploader(object):
	def __init__(self, s3conf, bucketname, threads=1, chunksize_mb=5, attempt_limit=5, timeout=10800):
		self.s3conf = s3conf # expects s3conf as `dict` with keys "aws_access_key_id" and "aws_secret_access_key" defined
		self._check_bucket(bucketname) # s3 bucket name
		self.chunksize = int(chunksize_mb * 2**20) # get chunksize in bytes
		self.attempt_limit = attempt_limit # default 5 attempts per chunk
		self.timeout = timeout # default 3 hours (10800 seconds)
		if threads <= 0: # number of threads
			raise ValueError("threads must be > 0")
		self.threads = threads # number of parallel threads for upload

	def _get_bucket(self, validate=False):
		conn = S3Connection(**self.s3conf)
		return conn.get_bucket(self.bucketname, validate=validate)

	def _check_bucket(self, bucketname):
		# throws an error if bucket does not exist
		self.bucketname = bucketname
		bucket = self._get_bucket(validate=True)

	def kill_old_multipart_uploads(self):
		bucket = self._get_bucket()
		for mp in bucket.get_all_multipart_uploads():
			mp.cancel_upload()

	def delete(self, keynames):
		bucket = self._get_bucket()
		logging.info("Deleting keys: %s" % keynames)
		return bucket.delete_keys(keynames)

	def _file_chunker(self, mp_id, filename):
		"""generator that returns tuples consumed by _upload_part worker function during multipart upload"""
		fsize = os.stat(filename).st_size # filesize, used to calculate chunk size
		sbyte = 0 # starting byte to read in file
		chunk_num = 1 # ordinal for chunks, indexed to 1

		while sbyte < fsize: # while the starting byte is less than the total number of bytes
			nbytes = min( [ self.chunksize, fsize - sbyte ] ) # number of bytes in chunk
			yield ( mp_id, self.bucketname, filename, self.attempt_limit, chunk_num, sbyte, nbytes )
			chunk_num += 1
			sbyte += nbytes # move starting byte to next position

	def _upload_part(self,args):
		"""Multipart Upload multiprocessing worker function, reads chunk from file and sends to S3"""
		# unpack arguments
		(mp_id, bucketname, filename, attempt_limit, chunk_num, sbyte, nbytes) = args
		attempts=0
		success=False
		while attempts < attempt_limit and not success: # keep trying upload until success or limit reached
			attempts += 1
			time.sleep(random.uniform(0.01, 0.49)) # be nice to your APIs, make request on average after .25 second wait
			logging.info("%s Uploading chunk %s. Attempt #%s" % (os.path.basename(filename), chunk_num, attempts) )
			try:
				with open(filename, 'rb') as ff:
					ff.seek(sbyte) # go to starting byte
					bb = ff.read(nbytes) # read chunk
					fp = io.BytesIO(bb) # write bytes to fp
					fp.seek(0) # set head to start of fp object
				
				bucket = self._get_bucket() 
				for mp in bucket.get_all_multipart_uploads():
					if mp.id == mp_id: # make sure chunk goes to correct key in S3
						mp.upload_part_from_file(fp=fp, part_num=chunk_num)

				logging.info( "%s Chunk %s successfully uploaded. %s Attempts." % (os.path.basename(filename), chunk_num, attempts) )
				success = True # break loop on success
			except Exception as exc:
				# fail gracefully so exception does not disrupt retry
				logging.info( "%s Chunk %s upload failed on attempt #%s." % (os.path.basename(filename), chunk_num, attempts) )
				logging.warn(exc)
		return (chunk_num, success)

	def upload(self, filename, keyname=None):
		"""sends file to S3 with optimal method, returns False on failure, True on success"""
		start_time = time.time() # for tracking how long an upload takes

		if keyname is None:
			keyname = os.path.basename(filename) # shorten keyname

		bucket = self._get_bucket()
		
		fsize = os.stat(filename).st_size
		if fsize < 5242880:
			# if file less than 5mb in size, revert to simple single-part upload
			logging.info("%s too small for multipart, reverting to simple upload" % keyname )
			key = bucket.new_key(keyname)
			key.set_contents_from_filename(filename)
			logging.info("%s Upload Complete!" % keyname )

		else:
			mp = bucket.initiate_multipart_upload(keyname) # initiate multipart upload

			fchunks = self._file_chunker(mp.id, filename) # create file chunk generator

			if self.threads == 1:
				# does not spawn any worker processes, safe for use in multithreaded application
				result = []
				try:
					for chunk in fchunks:
						part_result = self._upload_part(chunk)
						result.append(part_result) # log to results
						if part_result[1] == False: # if upload fails, break loop
							logging.warning("%s part failed to upload. Cancelling Multipart Upload." % keyname)
							mp.cancel_upload()
							return None
				except Exception as exc:
					logging.warning("%s failed to upload - general. Cancelling Multipart Upload." % keyname, exc)
					mp.cancel_upload()
					return None

			else:
				# spawns multiprocessing worker processes, cannot be used within a Process or Thread object because of GIL. Drop thread count to 1.

				pool = multiprocessing.Pool(self.threads) # initialize pool with n threads

				# pass generator and helper function to pool, execute asynchronously
				run_upload = pool.map_async(func=self._upload_part, iterable=fchunks)

				# get results of upload as list of tuples, or cancel upload on error (e.g. timeout error)
				try:
					result = run_upload.get(timeout=self.timeout)
				except Exception as exc:
					logging.warning("%s failed to upload. Cancelling Multipart Upload." % keyname)
					mp.cancel_upload()
					return None

			# ensure all chunks uploaded successfully, or cancel upload if success=False for any value
			for chunk_num, success in result:
				if not success:
					logging.warning("Chunk %s failed to upload" % chunk_num)
					mp.cancel_upload()
					return None

			# ensure S3 has collected all parts of upload, compare to number of chunks uploaded
			if len(mp.get_all_parts()) == len(result):
				try:
					mp.complete_upload()
					logging.info("%s Upload Complete!" % keyname )
				except Exception as exc:
					mp.cancel_upload()
					logging.warning("%s failed to upload compltely. Cancelling Multipart Upload." % keyname)
					return None
			else:
				mp.cancel_upload()
				logging.warning("%s failed to upload completely. Cancelling Multipart Upload." % keyname)
				return None

		end_time = time.time()
		upload_speed_mbps = ( fsize / 2 **20 ) / ( end_time - start_time )

		logging.info("Upload Speed: %f MB/s" % ( upload_speed_mbps ))
		return keyname
