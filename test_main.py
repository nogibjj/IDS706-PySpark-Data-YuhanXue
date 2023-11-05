from pyspark.sql.session import SparkSession
import pytest
from mylib.lib import (
    initiate_spark_session,
    read_dataset,
    describe,
    transform_bmi,
)


@pytest.fixture(scope="session")
def spark_session():
    session = initiate_spark_session("TestDiabetesDataProcessing")
    yield session
    session.stop()


def test_data_loading(spark_session: SparkSession):
    data_path = "diabetes.csv"
    diabetes_df = read_dataset(spark_session, data_path)
    assert diabetes_df is not None and diabetes_df.count() > 0


def test_data_describe(spark_session: SparkSession):
    diabetes_df = read_dataset(spark_session, "diabetes.csv")
    description_data = describe(diabetes_df)
    assert description_data is not None


def test_data_transform(spark_session: SparkSession):
    diabetes_df = read_dataset(spark_session, "diabetes.csv")
    transformed_diabetes_df = transform_bmi(diabetes_df)
    assert transformed_diabetes_df is not None
    assert "BMICategory" in transformed_diabetes_df.columns


def run_tests():
    session = spark_session()
    test_data_loading(session)
    test_data_loading(session)
    test_data_transform(session)


if __name__ == "__main__":
    run_tests()
