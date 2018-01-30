import os
import re
import sys
import csv
import math
from subprocess import Popen, PIPE
from os import listdir
from os.path import isfile, join

def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=30):
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '>' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write('\r%s >%s> %s%s %s' % (prefix, bar, percents, '%', suffix)),
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

def main(_args):

	file_size_lower_tresh = 1000000
	file_size_upper_tresh = 4400000
	file_count_tresh = 1000

	file_size_good = None
	file_size_below = None

	_prog = 0

	with open(_args[2], 'rb') as df:
		reader = csv.reader(df)
		_csv_list = list(reader)

	_meta_df = _csv_list[0][2:len(_csv_list[0])-1]
	_meta_df.extend(['SizeSmall', 'SizeWithinTresh', 'FileSize', 'FileCount', 'label'])

	_csv_out = _args[2].split('.')
	with open(_csv_out[0] + '_out.' + _csv_out[1], 'wb') as _out, open('extract.log', 'ab') as _log:
		wr = csv.writer(_out, quoting=csv.QUOTE_NONE)
		wr.writerow(_meta_df)
		_log.write('------------------------------------------------------------------\n')
		_log.write('Parsing data from: ' + _csv_out[0] + '.' + _csv_out[1] + '\n')
		print_progress(_prog, len(_csv_list) - 1, prefix = 'Progress:', suffix = 'Complete')

		for x in range(1, len(_csv_list)):
			if os.path.isfile(os.path.join(_args[1], _csv_list[x][0])):
				_meta_row = []
				
				_fsize = os.path.getsize(os.path.join(_args[1], _csv_list[x][0]))
				if _fsize < file_size_lower_tresh:
					file_size_below = 1
				else:
					file_size_below = 0
				if _fsize > file_size_upper_tresh:
					file_size_good = 1
				else:
					file_size_good = 0

				_7z_proc = Popen(['7z', 'l', os.path.join(_args[1], _csv_list[x][0]), '-p'], stdout=PIPE, stderr=PIPE)
				_7z_out, _7z_err = _7z_proc.communicate()

				if bool(_7z_err):
					#unsupported/corrupted?
					_fcon_ct = -1
				else:
					if bool(_7z_out):
						_7z_num = re.search('[0-9]+[ ]files', _7z_out)
						_7z_pwd = re.search('Wrong password', _7z_out)
						if bool(_7z_num):
							_fcon_ct = _7z_num.group().split()[0]
						elif bool(_7z_pwd):
							#passprot
							_fcon_ct = -5
						else:
							#nofilecount
							_fcon_ct = -1
					else:
						#empty
						_fcon_ct = -1

				_meta_row.extend(_csv_list[x][2:len(_csv_list[x])-1])
				_meta_row.append(file_size_below)
				_meta_row.append(file_size_good)
				_meta_row.append(_fsize)
				_meta_row.append(_fcon_ct)
				_meta_row.extend(_csv_list[x][len(_csv_list[x])-1:])
				
				wr.writerow(_meta_row)
				
				_log.write(_csv_list[x][0] + ', ' + _csv_list[x][1] + ', ' + str(file_size_below) + ', ' + str(file_size_good) + ', ' + str(
					_fcon_ct) + ' [ERR] ' + _7z_err + '\n')
			_prog += 1
			print_progress(_prog, len(_csv_list) - 1, prefix = 'Progress:', suffix = 'Complete')

if __name__ == "__main__":
	if len(sys.argv) > 2:
		main(sys.argv)
	else:
		print "usage: extract.py [/path/to/files/] [csv_filename]"
