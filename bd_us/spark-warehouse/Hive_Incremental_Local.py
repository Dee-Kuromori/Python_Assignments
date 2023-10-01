from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyhive import hive
# .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf()) \
warehouse_location = "/warehouse/tablespace/managed/hive"
spark_postgres = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.jars", r"C:\Users\Consultant\Downloads\postgresql-42.6.0.jar") \
.config("spark.sql.catalogImplementation","hive") \
.enableHiveSupport() \
.getOrCreate()


# df = spark_hive.read \
#     .jdbc(jdbc_url, "fraud_full_load_external")

postgres_df = spark_postgres.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", "frauddetection") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("driver", "org.postgresql.Driver") \
    .load()

postgres_df.printSchema()


#####hive connection####

# Create a connection to Hive
connection = hive.Connection(
    host='localhost',
    port=10000, # Default HiveServer2 port
    database='fraud_project'  # Optional, specify the default database
    # username='your_username',  # Optional, if authentication is required
    # password='your_password',  # Optional, if authentication is required

)

cursor = connection.cursor()
cursor.execute("SELECT MAX(CAST(row_id AS INT)) FROM fraud_full_load_external")

table = cursor.fetchall()

for column in table:
    print(column)


# spark_hive.sql("use fraud_project").show()
