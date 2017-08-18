# Mondrian k-anonymity in Spark

Spark implementation of Mondrian, a k-anonymity algorithm.

## Running an example with PySpark

From this project's root directory and assuming `pyspark` and `spark-submit` are already on your environment's path:

1. Package the scala project containing the Mondrian implementation:

```
./build/sbt clean package
```

2. Submit the application (Siape or Titanic) with `spark-submit` (also from root directory):

```
spark-submit --jars target/scala-2.11/k-anonymity-mondrian_2.11-1.0.jar src/main/python/app_runner.py \
            <example> <csv_path> <key_columns> <sensitive_columns> <k> <mode>
```

where:

- **example**: `siape` or `titanic`
- **csv_path**: complete path (file:// or hdfs://) containing the siape csv file
- **key_columns**: comma-separated list of columns that together will be used as k-anonymity identifier. Examples:
  `faixa_etaria,habilitacao_profissional,uf_residencial,funcao`, `age,cabin`.
- **sensitive_columns**: comma-separated list of columns that are to be considered as sensitive information. Examples:
  `remuneracao`, `survived`.
- **k**: k-anonymity parameter
- **mode**: `strict` or `relaxed`
