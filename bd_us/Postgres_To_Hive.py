from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession \
    .builder \
    .appName("Postgres Table Load") \
    .master("local") \
    .enableHiveSupport().getOrCreate()
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", "frauddetection_sample") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("driver", "org.postgresql.Driver") \
    .load()


df.show()

database_name = "fraud_db"
table_name = "fraud_full_load_external"

df.write.mode("overwrite").saveAsTable(database_name+"."+table_name)

# Stop the SparkSession
# spark.stop()





