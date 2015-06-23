Spark SQL make easy
===================

Spark-SQL is awesome and I kindly wanted to use it for everything. Well probably not everything, but for a lot of things of BigData reports. ;) .

sqle is a python command line tool that intended to make Spark-SQL easy.

Let say you have a file ```table.sql```. For people who familiar with python might already notices that there are some template variables in the file.

```SQL
CREATE temporary table TBL_A
USING com.databricks.spark.csv
OPTIONS (path "{A}", header "true");

CREATE temporary table TBL_B
USING com.databricks.spark.csv
OPTIONS (path "{B}", header "true");

```

And you want to run a query:

```SQL
SELECT A.name, B.age from A INNER JOIN B on A.id = B.id
```

With ```sqle```, you can simply do:

```
spark-submit sqle.py table.sql 'SELECT A.name, B.age from A INNER JOIN B on A.id = B.id'
```
