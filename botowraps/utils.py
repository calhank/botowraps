#!/usr/local/bin/python3

# standard lib imports
import os
import gzip
import shutil
import subprocess

# functions
def gzc(filename):
	# from gzip documentation
	fo_name = filename+".gz"
	with open(filename, 'rb') as fi:
		with gzip.open(fo_name, 'wb') as fo:
		    shutil.copyfileobj(fi, fo)
	return fo_name


def split_csv_by_row(filename, rows_per_file=10000, target_dir=None, header_action="na"):
	"""method that takes .csv file and splits it by rows into smaller files with a suffix .n after the filename. Assumes at most 1 header row"""
	## input error handling
	header_opts = ["delete","keep", "na"]
	if header_action not in header_opts:
		raise ValueError("arg header_action must be one of: [%s]" % ", ".join(header_opts) )

	new_filename = filename
	if target_dir is not None:
		new_filename = os.path.join( target_dir, os.path.basename(filename) )

	output_file_list = []

	filenum=1
	first_row=True
	with open(filename, "rb") as original:

		output_file_name = new_filename + "." + str(filenum)
		out = open(output_file_name, "wb")
		output_file_list.append(output_file_name)

		rows=0
		for line in original: 
			rows += 1

			if first_row: # if you're at the top of the new file
				first_row=False
				if header_action == "keep":
					header = line
					out.write(header)
					continue

				elif header_action == "delete":
					continue

			if rows > rows_per_file: # condition to open new file
				rows=0
				filenum += 1
				out.close()
				output_file_name = new_filename + "." + str(filenum)
				out = open(output_file_name, "wb")
				output_file_list.append(output_file_name)
			
				if header_action == "keep":
					rows += 1
					out.write(header)

			out.write(line)

		out.close()

	return output_file_list


### DEPRECATED ###

# def gz(filename, keep=True):
# 	# method to gzip files/dirs in python3
# 	cmd = ["gzip", "-f", filename]
# 	if keep:
# 		cmd.insert(2,"-k")
# 	subprocess.call(cmd)
# 	return(filename+".gz")

