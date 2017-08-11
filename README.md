# Mondrian k-anonymity in Spark

Spark implementation of Mondrian, a k-anonymity algorithm.


## Running an example with PySpark

From this project's root directory and assuming `pyspark` and `spark-submit` are already on your environment's path:

1. Package the scala project containing the Mondrian implementation:

```
./build/sbt clean package
```

2. Submit the application (Siape) with `spark-submit` (also from root directory):

```
spark-submit --jars target/scala-2.11/k-anonymity-mondrian_2.11-1.0.jar src/main/python/siape_app_runner.py \
            <siape_csv_path> <key_columns> <sensitive_columns> <k> <mode>
```

where:

- **siape_csv_path**: complete path (file:// or hdfs://) containing the siape csv file
- **key_columns**: comma-separated list of columns that together will be used as k-anonymity identifier. Example:
  `faixa_etaria,habilitacao_profissional,uf_residencial,funcao`.
- **sensitive_columns**: comma-separated list of columns that are to be considered as sensitive information.
- **k**: k-anonymity parameter
- **mode**: `strict` or `relaxed`
