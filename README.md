# botowraps

## Functions

###botowraps.utils

* ```gzc( filename )``` - Takes a filename as argument, compresses that file with gzip, returns filename with '.gz' extension on completion
* ```split_csv_by_row( filename, [rows_per_file=10000, [target_dir=None, [header_action="na"]]])``` - splits .csv file into chunks. By default splits file into 10000 row chunks, saves chunks to same directory as original file. 'header_action' takes a string of either 'delete', 'keep', or 'na'. 'delete' deletes the header from the first chunk and all subsequent chunks. 'keep' retains header in first chunk and prepends to all subsequent chunks. 'na' does nothing. 'na' is default. The header is assumed to be a single row ending in a newline character.

###botowraps.s3

* ```S3Uploader``` - a class that assists in uploading and deleting objects from an AWS S3 bucket. 


... more docs later ...
