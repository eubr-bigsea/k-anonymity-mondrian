# !/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from pyspark.sql import SparkSession

# arguments
try:
    siape_path = sys.argv[1]        # local or hdfs
    key_columns = sys.argv[2]       # col1,col2,col3, ...
    sensitive_columns = sys.argv[3] # col1,col2,col3, ...
    k = sys.argv[4]                 # k-anonymity
    mode = sys.argv[5]              # strict | relaxed
except:
    print "%s%s%s" % ("Usage: python siape_app_runner.py <siape_csv_path>",
          " <key_columns_sep_by_comma> <sensitive_columns_sep_by_comma>",
          " <k> <mode: strict|relaxed>")
    sys.exit(1)

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
jspark = spark._jsparkSession
jvm = spark.sparkContext._jvm

# format arguments
key_columns = key_columns.split(',')
sensitive_columns = sensitive_columns.split(',')
k = int(k)

print """Running Mondrian with the following parameters: {
    siape_path=%s
    key_columns=%s
    sensitive_columns=%s
    k=%s
    mode=%s
}""" % (siape_path, key_columns, sensitive_columns, k, mode)

# input dataset
raw_data = jvm.br.ufmg.cs.lib.privacy.kanonymity.examples.SiapeApp.\
        readSiape(jspark, siape_path)

# configure algorithm
mondrian = jvm.br.ufmg.cs.lib.privacy.kanonymity.Mondrian(
        raw_data, key_columns, sensitive_columns, k, mode)

# get result
mondrian_res = mondrian.result()

print 'Normalized Certainty Penalty (NCP): %.2f%%' % mondrian_res.ncp()

# show indexed result datase
mondrian_res.resultDataset().show()

# show converted result dataset
mondrian_res.resultDatasetRev().show()

spark.stop()
