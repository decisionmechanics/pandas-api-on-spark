# Pandas API on Spark

- Build distributed pipelines with pandas-like syntax
- Learn by doing: short exercises throughout
- Target audience: pandas users scaling to big data

## Initialization

### Install PySpark

```bash
docker run --name pyspark -p 8888:8888 -d --rm quay.io/jupyter/pyspark-notebook:spark-4.0.0   
```

## Conceptual foundations

### What is the Pandas API on Spark?

- pandas-on-Spark = pandas-compatible API backed by Spark
- Executed on cluster, but code resembles pandas
- Originally Koalas project---now part of PySpark
- Seamless transition from local analysis to distributed scale

```python
from pyspark.sql import SparkSession
import pyspark.pandas as ps

spark = SparkSession.builder.getOrCreate()

print("Spark version:", spark.version)
print("pyspark.pandas version:", ps.sys.version)

print(ps.options.display.max_rows)
```

### Why use the Pandas API?

- Scale pandas workloads without major rewrite
- 1Leverage Spark for parallelism, cluster resources
- Integrate with Spark SQL, MLlib, Delta Lake
- Use familiar DataFrame operations without learning PySpark immediately

### Alternatives

- Native PySpark DataFrame API
  - Most performant
  - SQL-style operations
  - More verbose but optimized for distributed computing
- pandas (single machine)
  - Fast for small/medium datasets
  - No cluster execution
- Other frameworks
  - Dask: parallel pandas for local clusters
  - Modin: API-compatible scaling using Ray/Dask
  - Polars: fast local columnar engine (not distributed)

### Pros of Pandas API on Spark

- Very low learning curve for pandas users
- Many pandas idioms (`loc`, `iloc`, `merge`, `value_counts`) available
- Can interoperate with PySpark when needed
- Good for prototyping or mixed-level Spark users

###  Cons of Pandas API on Spark

- Not yet full pandas coverage
- API translation layer can slow some operations
- Complex operations may run differently than expected
- Small-data overhead: slower than pandas on small files
- Some pandas behaviors are subtly different in Spark

## Cluster and data access

### Connecting to a Spark cluster

- Create a SparkSession with pandas API enabled
- Local mode (`local[*]`) will use (all cores) on your local machine
  - Use the correct syntax to connect to your cluster (YARN, Kubernetes, Databricks)

```python
from pyspark.sql import SparkSession
import pyspark.pandas as ps

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sql("set spark.sql.ansi.enabled=false")

df = ps.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
print(type(df))
df.head()
```

### Loading data

- Load CSV: `ps.read_csv()`
- Load Parquet: `ps.read_parquet()`
- pandas API will infer the schema automagically
- Types would need to be corrected after reading
- PySpark can be more predictable in production environments
  - Supports explicit schemas

```python
shark_incidents_dirty_df = ps.read_csv("shark-incidents.csv", index_col="UIN")
shark_incidents_df = ps.read_parquet("shark-incidents.parquet", index_col="uin")

shark_incidents_df.head()
```

### Creating dataframes programmatically

- From Python dicts/lists
- From local pandas DataFrames using `ps.from_pandas()`

```python
import pandas as pd

data = {"name": ["Alice", "Bob"], "age": [25, 32]}
df_spark = ps.DataFrame(data)

df_local = pd.DataFrame({"city": ["London", "Paris"], "country": ["UK", "FR"]})
df_from_pandas = ps.from_pandas(df_local)

df_spark.head()
df_from_pandas.head()
```

### Inspecting data

- `df.head()`, `df.tail()`
- `df.info()` (differences from pandas)
- `df.describe()`
- Indexes in the Pandas API are mostly for API compatibility
- Spark uses its own SQL types

```python
shark_incidents_df.head(5)
shark_incidents_df.info()
shark_incidents_df.describe()
```

## Transformations

### Selecting and filtering

- Column selection: `df["col"]`, `df[["col1", "col2"]]`
- Row filtering: boolean conditions
- Comparison to PySpark: intuitive vs explicit

```python
# Select a few columns
subset_df = shark_incidents_df[["shark_common_name", "victim_injury"]]
subset_df.head()

# Filter rows
fatal_df = subset_df[subset_df["victim_injury"] == "fatal"]
fatal_df.head()
```

### Column expressions

- Add column: `df["new"] = df["a"] + df["b"]`
- Casting types: `df["a"].astype("int")`
- replace, clip
- Pipeline-like chaining

```python
shark_incidents_df["was_fatal"] = shark_incidents_df["victim_injury"] == "fatal"
shark_incidents_df.head()
```

### Handling missing data

- Detect nulls: `df.isnull().sum()`
- dropna with options
- fillna with scalars and dicts
- Spark only has a single null value
  - pandas has `None`, `NaN` and `pd.NA`

```python
shark_incidents_dirty_df.tail()
shark_incidents_dirty_df.isnull().sum(axis=0)

clean_df = shark_incidents_dirty_df.dropna(subset=["Shark.common.name"])
clean_df.tail()
```

### Sorting and renaming

- `df.sort_values("col")`
- Ascending/descending options
- Renaming: `df.rename(columns={"old":"new"})`
- In-place semantics differences
- Exercise: reformat a messy dataset

```python
longest_df = shark_incidents_df.sort_values("shark_length_m", ascending=False)
longest_df.head()

renamed_df = shark_incidents_dirty_df.rename(columns={ "Shark.common.name": "species" })
renamed_df.head()
```

## Aggregations, joins and dates

### GroupBy and aggregations

- groupby object
- Single and multiple aggregations

```python
length_df = (
    shark_incidents_df.groupby("shark_common_name")
    .agg(largest=("shark_length_m", "max"), average=("shark_length_m", "median"))
    .sort_values(["average"], ascending=False)
)

length_df.head()
```

### Joins

- join vs merge
  - merge is more like SQL joins
- Supported join types
  - inner, left, right, outer, cross
- PySpark joins can use expressions
- Can use index in merge

```python
shark_incidents_df.merge(
    length_df, how="left", left_on="shark_common_name", right_index=True
).head()
```

### Dates and times

- Convert using `ps.to_datetime`
- Extract fields: year, month, day
- Filtering date ranges
- Example: time-based grouping
- Exercise: daily aggregation from timestamped data

```python
shark_incidents_df["incident_day"] = 1

shark_incidents_df["incident_date"] = ps.to_datetime(
    shark_incidents_df.rename(
        columns={
            "incident_day": "day",
            "incident_month": "month",
            "incident_year": "year",
        }
    )
).head()

shark_incidents_df.head()

shark_incidents_df.dtypes
```

## Advanced operations

### Window functions

- Window specification
- ranking: rank, dense_rank
- rolling: rolling mean, rolling sum
- running totals via cumulative ops
- Example: compute per-group running sum
- Exercise: ranking within groups

```python
shark_incidents_df["length_rank"] = shark_incidents_df.groupby("shark_common_name")[
    "shark_length_m"
].rank(method="dense", ascending=False)

shark_incidents_df.head()
```

### Interoperating with PySpark

- Convert: `df.to_spark()` and back
- Use when needing advanced Spark SQL capabilities

```python
from pyspark.sql import functions as F

shark_incidents_sdf = shark_incidents_df.to_spark(index_col="uin")
shark_incidents_sdf.printSchema()

shark_incidents_extended_sdf = shark_incidents_sdf.withColumn(
    "shark_length_ft", F.col("shark_length_m") * 3.28084
)

shark_incidents_extended_df = shark_incidents_extended_sdf.pandas_api(index_col="uin")
shark_incidents_extended_df.sort_values("shark_length_m", ascending=False).head()
```

## Performance and exporting

### Performance considerations

- Understand lazy vs eager execution
- Avoid `collect` and `to_pandas` for large data
- Use cache and persist when reusing DataFrames
- Repartitioning for performance
- When working with lots of data, PySpark will have better performance

```python
# Caching underlying Spark DataFrame
shark_incidents_df.cache()

# Repartitoning for performance
shark_incidents_sdf = shark_incidents_df.to_spark()
shark_incidents_repartitioned_sdf = shark_incidents_sdf.repartition("shark_common_name")
shark_incidents_repartitioned_df = shark_incidents_repartitioned_sdf.pandas_api()
```

### Exporting data

- Write to Parquet, CSV, Delta
- Specify mode (overwrite, append)
- Write partitions using Spark's distributed write

```python
shark_incidents_df.to_parquet("parquet/", index_col="uin")
shark_incidents_df.to_csv("csv/", index_col="uin")
```