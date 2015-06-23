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

```BASH
sqle.py table.sql 'SELECT A.name, B.age from A INNER JOIN B on A.id = B.id' -x A=s3://mybk/a B=s3://mybk/b --dry
```

It will output to stdio as:
```SQL
-- stmt[idx=0, src=table.sql]:
CREATE temporary table TBL_A
USING com.databricks.spark.csv
OPTIONS (path "s3://mybk/a", header "true");

-- stmt[idx=1, src=table.sql]:
CREATE temporary table TBL_B
USING com.databricks.spark.csv
OPTIONS (path "s3://mybk/b", header "true");

-- stmt[idx=2, src=<FROM ARGS>]:
select * from A;
```

