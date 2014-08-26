#!/usr/bin/python

import sys
import subprocess
import re
import time
import datetime

print "Executing TPC DS benchmark for Hive... "

# run as hdfs user

#Specifically executing queries used in Best Buy benchmark
#in testbench.settings file make the following modifications:
# Note: the lines below do not exist and need to be added
# Note: the number is the scale factor specified setup script
#1.    use tpcds_bin_partitioned_orc_30000; 
#2.    tez.queue.name=apps
# Note: the lines below exist already and need to be modified
#3.    set hive.tez.container.size=8192; 
#4.    set hive.tez.java.opts=-Xmx7680m;
#5.    set hive.execution.engine=tez;
 

queriesToRun = set([
55,
27,
52,
42,
7,
15,
26,
19,
3,
96,
43,
82,
84,
12,
58,
40,
68,
66,
95,
93,
21,
46,
88,
32,
17,
94,
76,
92,
97,
87,
50,
20,
25,
28,
45,
49,
85,
89,
90,
73,
71,
34,
39,
64,
98])

ts = time.time()
datetimestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')
filename = "results_" + datetimestamp + ".out"
f = open(filename,'w')
f.write("querynum,time\n")
for i in sorted(queriesToRun):
        print "Running query #" + str(i)
        filename = "query" + str(i) + ".sql"
        command = "hive -i testbench.settings -f " + filename
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, bufsize=-1)
        (out, err) = proc.communicate()
        print "-------------------"
        print "stderr of command: " + err
        timeval = re.search('(?<=taken:)\W\d\d\D\d\d',err)
        if timeval:
                f.write(str(i) + "," + timeval.group(0).strip() + "\n")
        else:
                f.write(str(i) + ",0\n")
		f.flush()
f.close()


