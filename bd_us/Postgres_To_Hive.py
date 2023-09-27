from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .master("local") \
    .enableHiveSupport().getOrCreate()
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", "fraudtable") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("driver", "org.postgresql.Driver") \
    .load()


df.show()
#
# # spark.sql("CREATE DATABASE fraud_db")
# spark.sql("SHOW DATABASES").show()
# spark.sql("USE fraud_db").show()
# # Assuming you already have an SQLContext
# # # Provide your SQL query as a string
# view_name = "fraud_view"
database_name = "fraud_db"
table_name = "fraud_table"
# #
#
# df.createOrReplaceTempView(view_name)
df.write.mode("overwrite").saveAsTable(database_name+"."+table_name)
# # spark.sql("drop table if exists "+table_name)
# spark.sql("CREATE TABLE if NOT EXISTS " + table_name+ " AS SELECT * FROM " + view_name)
# df.write.saveAsTable("fraud_spark",mode= "overwrite")
# df.write.save("fraud_spark", format="hive")

# Stop the SparkSession
# spark.stop()





