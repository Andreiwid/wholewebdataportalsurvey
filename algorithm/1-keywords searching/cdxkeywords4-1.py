import gzip
import shutil
import hashlib
import datetime
import os
import json

#function to find the Nth ocorrence of a substring
#necessary to find single slash '/' of a URL and not make confusion with double slash '//'
def findnth(string, substring, n):
    parts = string.split(substring, n + 1)
    if len(parts) <= n + 1:
        return -1
    return len(string) - len(parts[-1]) - len(substring)

#set keywords to find in URLs
keywords=["data","dados","datos","daten","dati"]

#set cdx counters
cdxcounter = 0
max_cdxcounter = 49

#an ID to unique identity this running
resultsid = "results4-1"

output_file0 = open(resultsid + "-URL0.txt", "w")
#head of results file
output_file0.write(str(datetime.datetime.now()) + "\n")
output_file0.write("---\n")

output_file1 = open(resultsid + "-URL1.txt", "w")
#head of results file
output_file1.write(str(datetime.datetime.now()) + "\n")
output_file1.write("---\n")

output_file2 = open(resultsid + "-URL2.txt", "w")
#head of results file
output_file2.write(str(datetime.datetime.now()) + "\n")
output_file2.write("---\n")

stats_file = open(resultsid + "-stats.txt", "w")

completed_lines_hash_url0 = set()
completed_lines_hash_url1 = set()
completed_lines_hash_url2 = set()

while cdxcounter <= max_cdxcounter:
	cdxfile = "cdx-" + "%05d" % cdxcounter
	cdxcounter += 1

	print("Current cdx file: " + cdxfile)
	stats_file.write("CDX file: " + cdxfile + " Start:" + str(datetime.datetime.now()))

	#decompress current cdx file
	print(str(datetime.datetime.now()) + ": Uncompressing " + cdxfile + ".gz...")
	with gzip.open(cdxfile + ".gz", "rb") as f_in:
	    with open(cdxfile + ".txt", "wb") as f_out:
	        shutil.copyfileobj(f_in, f_out)
	print(str(datetime.datetime.now()) + ": Uncompressing " + cdxfile + ".gz completed!")

	#only a line position counter to check algorithm is running
	linepos = 1
	numberurlsfoundurl0 = 0
	numberurlsfoundurl1 = 0
	numberurlsfoundurl2 = 0

	with open(cdxfile + ".txt") as f_inline:
		print(str(datetime.datetime.now()) + ": Iterating " + cdxfile + ".txt...")
		for line in f_inline:
			#print line position counter in the console
			linepos += 1
			if linepos % 10000 == 0:
				print('passou ' + str(linepos) + '...')

			#verify few cases where there are {"url":} outside JSON part of the line
			if line.count("{\"url\":") > 1:
				continue
			
			#raises error if JSON has any problem 
			try:
				jsonline = json.loads(line[line.find("{\"url\":"):])
			except Exception as e:
				continue

			#gets from JSON URL and MIME values
			url = jsonline["url"]
			mime = jsonline["mime"]

			#only go ahead if mime type is text/html
			if mime == "text/html":
				#separate URL subfolders until 2nd deepth
				url0 = ''
				url1 = ''
				url2 = ''

				url1stsingleslashpos = findnth(url, "/", 2)
				url2sndingleslashpos = findnth(url, "/", 3)
				url3rddingleslashpos = findnth(url, "/", 4)

				if url1stsingleslashpos != -1:
					url0 = url[0:url1stsingleslashpos]

				if url2sndingleslashpos != -1:
					url1 = url[0:url2sndingleslashpos]

				if url3rddingleslashpos != -1:
					url2 = url[0:url3rddingleslashpos]

				#search for keywords only in url0
				if any(keyword in url0 for keyword in keywords):
					hashUrl0 = hashlib.md5(url0.encode("utf-8")).hexdigest()
					if hashUrl0 not in completed_lines_hash_url0:
						numberurlsfoundurl0 += 1
						#add a not duplicated URL in the output file
						output_file0.write(url0 + "\n")
						completed_lines_hash_url0.add(hashUrl0)

					hashUrl1 = hashlib.md5(url1.encode("utf-8")).hexdigest()
					if hashUrl1 not in completed_lines_hash_url1:
						numberurlsfoundurl1 += 1
						#add a not duplicated URL in the output file
						output_file1.write(url1 + "\n")
						completed_lines_hash_url1.add(hashUrl1)

					hashUrl2 = hashlib.md5(url2.encode("utf-8")).hexdigest()
					if hashUrl2 not in completed_lines_hash_url2:
						numberurlsfoundurl2 += 1
						#add a not duplicated URL in the output file
						output_file2.write(url2 + "\n")
						completed_lines_hash_url2.add(hashUrl2)

		print(str(datetime.datetime.now()) + ": Iteration " + cdxfile + ".txt completed!")

		stats_file.write(" End:" + str(datetime.datetime.now()) + " Processed lines: " + str(linepos) + " Number of URLs found (unique): " + str(numberurlsfoundurl0) + '|' + str(numberurlsfoundurl1) + '|' + str(numberurlsfoundurl2) + '\n')
		f_inline.close()

		#delete current uncompressed txt file
		if os.path.exists(cdxfile + ".txt"):
		    os.remove(cdxfile + ".txt")

output_file0.close()
output_file1.close()
output_file2.close()
stats_file.close()