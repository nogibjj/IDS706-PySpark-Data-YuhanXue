from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import (
    StructType, 
    StructField, 
    FloatType, 
    IntegerType)

REPORT_FILE = "report.md"

def add_to_report(description, content, sql_query=None):
    with open(REPORT_FILE, "a") as report:
        report.write(f"## {description}\n\n")
        if sql_query:
            report.write(f"**SQL Query:**\n```sql\n{sql_query}\n```\n\n")
        report.write("**Result Preview:**\n\n")
        report.write(f"```markdown\n{content}\n```\n\n")


def initiate_spark_session(app_title):
    session = SparkSession.builder.appName(app_title).getOrCreate()
    return session


def read_dataset(spark, dataset_path):
    diabetes_schema = StructType([
        StructField("Pregnancies", IntegerType(), True),  
        StructField("Glucose", IntegerType(), True),  
        StructField("BloodPressure", IntegerType(), True),  
        StructField("SkinThickness", IntegerType(), True),  
        StructField("Insulin", IntegerType(), True),  
        StructField("BMI", FloatType(), True), 
        StructField("DiabetesPedigreeFunction", FloatType(), True), 
        StructField("Age", IntegerType(), True),  
        StructField("Outcome", IntegerType(), True),  
    ])
    dataset = spark.read.option("header", 
    "true").option("sep", ",").schema(diabetes_schema).csv(dataset_path)
    add_to_report("Data Loading", dataset.limit(10).toPandas().to_markdown()) 
    return dataset


def describe(dataset):
    description = dataset.describe()
    add_to_report("Data Description", description.toPandas().to_markdown())
    return description


def transform_bmi(dataset):
    bmi_conditions = [
        (col("BMI") < 18.5),
        (col("BMI") > 23.5)
    ]
    bmi_categories = ["Lean", "Normal", "Obese"]
    transformed_dataset = dataset.withColumn("BMICategory", when(
        bmi_conditions[0], bmi_categories[0]
        ).when(bmi_conditions[1], bmi_categories[2]
        ).otherwise(bmi_categories[1]))
    add_to_report("Data Transformation", 
                     transformed_dataset.limit(10).toPandas().to_markdown())
    return transformed_dataset
