# spark-custom

Some useful customized classes for spark

## VariableLengthBinaryInputFormat

### Usage in Pyspark

Run pyspark with `variablelengthbinaryinputformat_2.11-0.1.jar`

```bash
pyspark --jars variablelengthbinaryinputformat_2.11-0.1.jar
```

Read records from binary files using `newAPIHadoopFile` method

```python
records = spark.sparkContext.newAPIHadoopFile("binary-file", "litchiware.spark.input.VariableLengthBinaryInputFormat", "org.apache.hadoop.io.NullWritable", "org.apache.hadoop.io.BytesWritable") 
print(records.count())
```
