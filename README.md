# IDS706-PySpark-Data-YuhanXue

![format workflow](https://github.com/nogibjj/IDS706-PySpark-Data-YuhanXue/actions/workflows/cicd.yml/badge.svg)


This repo contains Python scripts for the IDS706 Mini Project 10.

## Getting Started
Before running Python script, please install all dependecies:
```
make install
```
To run PySpark script, do:
```
python3 main.py
```
To test, do
```
make test
```
To format and lint, do:
```
make format && make lint
```

## Project Description
- **Data Loading:** Load data into a Spark DataFrame with schema.
- **Data Description**: Calculate the dataset statistics, including mean, median, and std dev.
- **Data Transformation:** Apply data transformation to clean and prepare the data for analysis. Turn BMI into one of the following: Lean, Normal, Obese.
- **Spark SQL Query:** Use Spark SQL to perform structured queries on the data, show the BMI average of people who have diabetes vs. don't have diabetes.
- **Data Summary Report:** Generate a summary report in markdown that highlights the above data manipulation.

After running the PySpark script, `report.md` will be generated.
