## Issue reading csv gz file Spark DataFrame

https://github.com/databricks/spark-csv/issues/436


### Workaround is to rename the column 
(inspired by https://stackoverflow.com/questions/34077353/how-to-change-dataframe-column-names-in-pyspark)

```python
%pyspark

from functools import reduce

data = spark.createDataFrame([("Alberto", 2), ("Dakota", 2)], 
                                  ["Name", "askdaosdka"])
data.show()
data.printSchema()

oldColumns = data.schema.names
print(oldColumns)

newColumns = oldColumns[:]
newColumns[1] = "Age"

print(newColumns)

df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), data)
df.printSchema()
df.show()
```

```sh
+-------+----------+
|   Name|askdaosdka|
+-------+----------+
|Alberto|         2|
| Dakota|         2|
+-------+----------+

root
 |-- Name: string (nullable = true)
 |-- askdaosdka: long (nullable = true)

['Name', 'askdaosdka']
['Name', 'Age']
root
 |-- Name: string (nullable = true)
 |-- Age: long (nullable = true)

+-------+---+
|   Name|Age|
+-------+---+
|Alberto|  2|
| Dakota|  2|
+-------+---+
```
