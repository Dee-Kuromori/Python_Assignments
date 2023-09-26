# Import necessary libraries
# Set the PYSPARK_PYTHON environment variable
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
python_executable = r"C:\Users\Consultant\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_PYTHON"] = python_executable

# Create a Spark session
spark = SparkSession.builder.appName("Transform_Datafrme").getOrCreate()

# Read a csv file
csv_df = spark.read.csv(r"C:\Users\Consultant\Documents\Assignments_training_bd\training_notes\week_9_25-9-29\sample_data.csv",header=True,inferSchema=True)
# print(type(csv_file))

#show column names
print(csv_df.schema.names)

# # Transformation 1: Selecting specific columns (name, age)
# csv_df.select(csv_df["Name"],csv_df["Age"]).show()
#
# # Transformation 2: Filtering rows based on a condition Age >30
# csv_df.where(csv_df["Age"] > 30).show()
#
#
# # Transformation 3: Adding a new column (Salary+10000)
# csv_df.withColumn("New_Salary",csv_df.Salary + 10000).show()
#
# # Transformation 4: Grouping and aggregating data, based on age calculate average salary
# csv_df.groupBy("Age").agg(f.avg("Salary")).show()
#
# # Transformation 5: Sorting by a column (order by age)
# csv_df.sort(csv_df.Age).show()
#
# # Transformation 6: Adding a new column with a conditional expression( if age >30 "Yes" else "No")
# csv_df.withColumn("Age_Group",f.when(f.col('Age') > 30, 'middle-aged').otherwise('young')).show()
#
# # Transformation 7: Dropping a column (drop salary)
# csv_df.drop("Salary").show()
#
# # Transformation 8: Renaming columns (name to full name)
# csv_df.withColumnRenamed("name","full_name").show()

# Transformation 9: Union of two DataFrames



df2 = spark.createDataFrame([("Mike", 40, 80000)], ["Name", "Age", "Salary"])
df2 = spark.createDataFrame([]
new_df = csv_df.union(df2)
new_df.show()


