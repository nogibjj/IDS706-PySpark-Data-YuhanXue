from mylib.lib import (
    initiate_spark_session,
    read_dataset,
    describe,
    transform_bmi,
    add_to_report,
)


def analyze_data():
    spark = initiate_spark_session("Diabetes Data Analysis")

    data_file_path = "diabetes.csv"
    diabetes_data = read_dataset(spark, data_file_path)
    description_data = describe(diabetes_data)
    description_data.createOrReplaceTempView("description_view")
    transformed_data = transform_bmi(diabetes_data)
    transformed_data.createOrReplaceTempView("diabetes_data_view")
    query_result = spark.sql(
        """
        SELECT Outcome, AVG(BMI) AS avg_bmi
        FROM diabetes_data_view
        GROUP BY Outcome
        """
    )

    query_result.show()
    add_to_report(
        "Spark SQL Query Result", query_result.limit(10).toPandas().to_markdown()
    )

    # query_result.write.format("csv").save("output/query_result.csv")

    spark.stop()


if __name__ == "__main__":
    analyze_data()
