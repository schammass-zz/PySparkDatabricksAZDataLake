# Databricks notebook source
#PYSPARK - connecting to AZ Data Lake and printing dataframe

from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark.sql.functions import* 

account_name = "demo2022admin"
container_name = "demo2022admin"
relative_path = "source"
adls_path = "abfss://%s@%s.dfs.core.windows.net/%s" % (container_name, account_name, relative_path)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net" %account_name, "SharedKey")
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" %account_name, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

df = spark.read.option('header','true') \
                .option('delimiter', ',') \
                .csv(adls_path + '/original.csv')
display(df)

# COMMAND ----------

#Display the Schema
df.printSchema()

# COMMAND ----------

#Correcting data type
df = df.withColumn("id", df["id"].cast("int")) \
                   .withColumnRenamed("City", "city") \
                   .withColumnRenamed("JobTitle", "job_title") \
                   .withColumnRenamed("Salary", "salary") \
                   .withColumn("Latitude", df["Latitude"].cast("float")) \
                   .withColumn("Longitude", df["Longitude"].cast("float")) \
                   .withColumnRenamed("Latitude", "latitude") \
                   .withColumnRenamed("Longitude", "longitude")

df.printSchema()

# COMMAND ----------

#Display changed dataframe
display(df)

# COMMAND ----------

#Create a new column "clean_city" with conditions
df = df.withColumn("clean_city", when(df.city.isNull(),'Unknown').otherwise(df.city))
display(df)

# COMMAND ----------

#Filter when the column "job_title" is not NULL
df = df.filter(df.job_title.isNotNull())
display(df)

# COMMAND ----------

#Create new column "clean_salary" using function substr() and changing the datatype from String to Float

df = df.withColumn('clean_salary',df.salary.substr(2,100).cast('float'))
display(df)

# COMMAND ----------

#Check the mean clean_salary

meanCleanSalary = df.groupBy().avg('clean_salary').take(1)[0][0]
print (meanCleanSalary)

# COMMAND ----------

#Check the mean clean_salary per clean_city

meanCleanSalaryPerCleanCityDf = df.groupBy("clean_city").avg('clean_salary')
display(meanCleanSalaryPerCleanCityDf)

# COMMAND ----------

#Create a new column 'new_salary'
df = df.withColumn('new_salary', when(df.clean_salary.isNull(), lit(meanCleanSalary)).otherwise(df.clean_salary))
display(df)

# COMMAND ----------

#Calculating the median
df.select(percentile_approx("latitude", [0.5], 1000000000).alias("median")).show(truncate=False)

# COMMAND ----------

#Create a new column "lat" when original column latitude is null fill with the median, otherwise fill with the value of the original one

df = df.withColumn('new_latitude', when(col('latitude').isNull(), lit('31.933973')).otherwise(df.latitude))
display(df)

# COMMAND ----------

#Calculate the average salary of the male and female.
import pyspark.sql.functions as sqlfunc
display(df.groupBy('gender').agg(sqlfunc.avg('new_salary').alias('AvgSalary')))

# COMMAND ----------

#Create two new columns "female_salary" and "male_salary" with conditions
df = df.withColumn('female_salary', when(df.gender == 'Female', df.new_salary).otherwise(lit(0))) \
    .withColumn('male_salary', when(df.gender == 'Male',df.new_salary).otherwise(lit(0)))
display(df)

# COMMAND ----------

#Calculate average female_salary and male_salary per job_title
averageSalaryPerSexPerJobTitle = df.groupBy('job_title').agg(sqlfunc.avg('female_salary').alias('average_female_salary'), sqlfunc.avg('male_salary').alias('average_male_salary'))
display(averageSalaryPerSexPerJobTitle)

# COMMAND ----------

#Calculate the delta (difference between average_female_salary and average_male_salary)
difAvgFemaleAvgMaleSalaryDF = averageSalaryPerSexPerJobTitle.withColumn('delta', averageSalaryPerSexPerJobTitle.average_female_salary - averageSalaryPerSexPerJobTitle.average_male_salary)
display(difAvgFemaleAvgMaleSalaryDF)
