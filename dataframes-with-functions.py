from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/alera/Documents/ARG/CURSOS/BIG DATA SPARK AND PYTHON/fakefriends-header.csv")

print("Here is our infered Schema: ")
people.printSchema()

print("Display name column: ")
people.select('name').show()

print("Filter anyone over 21: ")
people.filter(people.age > 21).show()

print("Filter anyone over 21: ")
people.groupBy("age").count().show()

print("Filter anyone over 21: ")
people.select(people.name, people.age + 10).show()

spark.stop()

#For run in terminal use: SCRIPTPATH$ /usr/local/Cellar/apache-spark/3.1.2/bin/spark-submit script_name.py 