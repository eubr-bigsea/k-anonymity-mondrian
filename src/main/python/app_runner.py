# !/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from pyspark.sql import SparkSession

def print_help_and_exit():
    print "%s%s%s" % ("Usage: python app_runner.py <titanic|siape> <csv_path>",
          " <key_columns_sep_by_comma> <sensitive_columns_sep_by_comma>",
          " <k> <mode: strict|relaxed>")
    sys.exit(1)

# arguments
try:
    example = sys.argv[1]           # name of the example [titanic, siape]
    path = sys.argv[2]              # local or hdfs
    key_columns = sys.argv[3]       # col1,col2,col3, ...
    sensitive_columns = sys.argv[4] # col1,col2,col3, ...
    k = sys.argv[5]                 # k-anonymity
    mode = sys.argv[6]              # strict | relaxed
except:
    print_help_and_exit()

if example not in ['titanic', 'siape'] or mode not in ['strict', 'relaxed']:
    print_help_and_exit()

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
jspark = spark._jsparkSession
jvm = spark.sparkContext._jvm

# format arguments
key_columns = key_columns.split(',')
sensitive_columns = sensitive_columns.split(',')
k = int(k)

print """Running Mondrian with the following parameters: {
    example=%s
    path=%s
    key_columns=%s
    sensitive_columns=%s
    k=%s
    mode=%s
}""" % (example, path, key_columns, sensitive_columns, k, mode)

# input dataset
if example == "siape":
    raw_data = jvm.br.ufmg.cs.lib.privacy.kanonymity.examples.SiapeApp.\
        readSiape(jspark, path)
elif example == "titanic":
    raw_data = jvm.br.ufmg.cs.lib.privacy.kanonymity.examples.TitanicApp.\
        readTitanic(jspark, path)

# configure algorithm
mondrian = jvm.br.ufmg.cs.lib.privacy.kanonymity.Mondrian(
        raw_data, key_columns, sensitive_columns, k, mode)

# get result
mondrian_res = mondrian.result()

# show indexed result datase
mondrian_res.resultDataset().show()

# show converted result dataset
mondrian_res.resultDatasetRev().show()

# show anonymized data
mondrian_res.anonymizedData().show()

print 'Normalized Certainty Penalty (NCP): %.2f%%' % mondrian_res.ncp()

spark.stop()
