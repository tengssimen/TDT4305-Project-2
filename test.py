from pyspark.sql import SparkSession
import graphframes
from pyspark.sql.context import SQLContext

spark = SparkSession.builder.master(
    "local[*]").appName("TDT4305 - Project Part 2").getOrCreate()
sc = spark.sparkContext

sc.addPyFile("graphframes-0.8.1-spark3.0-s_2.12.jar")

sqlContext = SQLContext(sc)


# Create a Vertex DataFrame with unique ID column "id"
v = sqlContext.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])


# Create a GraphFrame
g = graphframes.GraphFrame(v, e)
