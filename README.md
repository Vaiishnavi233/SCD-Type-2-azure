# SCD-Type-2-
SCD Type 2 Project Documentation
Project Title
SCD Type 2 Implementation Using Azure Databricks, PySpark, Azure Blob Storage, Azure SQL Database, and Azure Data Factory
________________________________________
1. Introduction
This project explains the implementation of SCD Type 2 using Azure services and PySpark.
Main purpose of project:
•	Read employee CSV files 
•	Store files in Azure Blob Storage 
•	Process data using Azure Databricks 
•	Compare old and new employee records 
•	Detect changed and new records 
•	Maintain historical employee data 
•	Load final data into Azure SQL Database 
This project is mainly used in:
•	ETL Process 
•	Data Warehouse 
•	Historical Data Tracking 
________________________________________
2. Technologies Used
Microsoft Azure : Cloud Platform
Azure Blob Storage : Store CSV files
Azure Databricks : Process data
PySpark : Data transformation
Azure SQL Database : Store final data
Azure Data Factory : Pipeline automation
________________________________________

3. Project Flow
CSV Files
   ↓
Azure Blob Storage
   ↓
Azure Databricks Notebook
   ↓
PySpark SCD Type 2 Logic
   ↓
Azure SQL Database
   ↓
Azure Data Factory Pipeline
________________________________________
4. Azure Blob Storage
Created Azure Blob Storage account.
Storage Account Name
vjtstoacct
Container Name
input
Purpose:
•	Store employee CSV files. 
________________________________________
5. CSV Files Used
First CSV File
File Name:
emp_21July2025.csv
Data
EmpID	EmpName	EmpSal	EmpAdd
11	Ajay	20000	Pune
12	Amit	25000	Mumbai
________________________________________
Second CSV File
File Name:
emp_23July2025.csv
Data
EmpID	EmpName	EmpSal	EmpAdd
11	Ajay	20000	Hyd
12	Amit	25000	Mumbai
13	Kawal	30000	Nasik
________________________________________
6. SQL Target Table
Table Name:
DimEmployee
SQL Query
CREATE TABLE DimEmployee
(
    Emp_key INT IDENTITY(1,1) PRIMARY KEY,
    EmpID INT,
    EmpName VARCHAR(100),
    EmpSal INT,
    EmpAdd VARCHAR(100),
    St_dt DATE,
    End_dt DATE
);
________________________________________
7. PySpark Code
Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
________________________________________
Storage Account Connection
spark.conf.set(
    "fs.azure.account.key.vjtstoacct.dfs.core.windows.net",
    "Storage_Account_Key"
)
________________________________________
Read First CSV File
initial_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("abfss://input@vjtstoacct.dfs.core.windows.net/emp_21July2025.csv")
________________________________________
Add Start Date and End Date
initial_df = initial_df.withColumn(
    "St_dt",
    lit("2025-07-21")
).withColumn(
    "End_dt",
    lit("9999-12-31")
)
________________________________________
6. Azure SQL Database
Created Azure SQL Database connection.
jdbc_url = "jdbc:sqlserver://vjysqlsrvr.database.windows.net:1433;database=db01"
connection_properties = {
    "user": "admin11",
    "password": "admin@11",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
________________________________________


Write Initial Data into SQL Table/Read Target Table
initial_df.write.jdbc(
    url=jdbc_url,
    table="DimEmployee",
    mode="overwrite",
    properties=connection_properties
)
________________________________________
Read Second CSV File
source_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("abfss://input@vjtstoacct.dfs.core.windows.net/emp_23July2025.csv")
________________________________________
Remove Duplicate Records
window_spec = Window.partitionBy("EmpID") \
    .orderBy(col("EmpAdd").desc())
source_latest_df = source_df.withColumn(
    "rn",
    row_number().over(window_spec)
).filter(
    col("rn") == 1
).drop("rn")
________________________________________
Read Target Table
target_df = spark.read.jdbc(
    url=jdbc_url,
    table="DimEmployee",
    properties=connection_properties
)
________________________________________
Filter Active Records
active_df = target_df.filter(
    col("End_dt") == "9999-12-31"
)
Logic
9999-12-31 means active current record
________________________________________
Join Source and Target Data
join_df = source_latest_df.alias("src").join(
    active_df.alias("tgt"),
    col("src.EmpID") == col("tgt.EmpID"),
    "left"
)
Logic
Used left join because:
•	Keep all source records 
•	Detect changed records 
•	Detect new records 
________________________________________
Identify Changed Records
changed_df = join_df.filter(
    (
        (col("src.EmpAdd") != col("tgt.EmpAdd")) |
        (col("src.EmpSal") != col("tgt.EmpSal")) |
        (col("src.EmpName") != col("tgt.EmpName"))
    ) &
    col("tgt.EmpID").isNotNull()
)
Logic
Checks:
•	Address changed 
•	Salary changed 
•	Name changed 
If changed:
•	Old record expires 
•	New record inserted 
________________________________________
Identify New Records
new_df = join_df.filter(
    col("tgt.EmpID").isNull()
)
Logic
Finds new employees not present in target table.
________________________________________
Get Changed Employee IDs
changed_keys_df = changed_df.select(
    col("src.EmpID").alias("EmpID")
).distinct()
________________________________________
Get Unchanged Records
unchanged_df = active_df.join(
    changed_keys_df,
    "EmpID",
    "left_anti"
)
Logic
Keeps records where no changes happened.
________________________________________
Expire Old Records
expired_df = active_df.join(
    changed_keys_df,
    "EmpID",
    "inner"
).withColumn(
    "End_dt",
    lit("2025-07-22")
)
Logic
Old records become inactive.
________________________________________
Insert Updated Records
updated_insert_df = changed_df.select(
    col("src.EmpID").alias("EmpID"),
    col("src.EmpName").alias("EmpName"),
    col("src.EmpSal").alias("EmpSal"),
    col("src.EmpAdd").alias("EmpAdd")
).withColumn(
    "St_dt",
    lit("2025-07-23")
).withColumn(
    "End_dt",
    lit("9999-12-31")
)
Logic
Insert latest updated employee records.
________________________________________
Insert New Records
new_insert_df = new_df.select(
    col("src.EmpID").alias("EmpID"),
    col("src.EmpName").alias("EmpName"),
    col("src.EmpSal").alias("EmpSal"),
    col("src.EmpAdd").alias("EmpAdd")
).withColumn(
    "St_dt",
    lit("2025-07-23")
).withColumn(
    "End_dt",
    lit("9999-12-31")
)
________________________________________
Create Final DataFrame
final_df = unchanged_df \
    .unionByName(expired_df) \
    .unionByName(updated_insert_df) \
    .unionByName(new_insert_df)
Logic
Combines:
•	Unchanged records 
•	Expired records 
•	Updated records 
•	New records 
________________________________________
Display Final Output
display(final_df)
________________________________________
Write Final Data into SQL Table
final_df.write.jdbc(
    url=jdbc_url,
    table="DimEmployee",
    mode="overwrite",
    properties=connection_properties
)

9. Final Output Table
Emp_key 	EmpID	EmpName	EmpSal	EmpAdd	St_dt	End_dt
1	11	Ajay	20000	Pune	2025-07-21	2025-07-22
2	12	Amit	25000	Mumbai	2025-07-21	9999-12-31
3	11	Ajay	20000	Hyd	2025-07-23	9999-12-31
4	13	Neha	30000	Delhi	2025-07-23	9999-12-31


________________________________________
10. Final SCD Type 2 Logic
Situation	Action
Data changed	Expire old record + Insert new record
New employee	Insert new record
No change	Keep existing active record
________________________________________
11. Azure Data Factory Pipeline
Created Azure Data Factory pipeline.
Pipeline Steps:
1.	Connect Databricks notebook 
2.	Trigger notebook execution 
3.	Read CSV files from Blob Storage 
4.	Process SCD Type 2 logic 
5.	Load final data into Azure SQL Database 
Purpose:
•	Automate complete ETL process. 
________________________________________
12. Advantages of SCD Type 2
•	Maintains history 
•	No data loss 
•	Tracks employee changes 
•	Useful for auditing 
•	Used in data warehouse projects 
________________________________________
13. Conclusion
This project successfully implemented SCD Type 2 using:
•	Azure Blob Storage 
•	Azure Databricks 
•	PySpark 
•	Azure SQL Database 
•	Azure Data Factory 
The system:
•	Reads employee CSV files 
•	Detects changed records 
•	Maintains history 
•	Expires old records 
•	Inserts updated records 
•	Loads final data into SQL Database 
•	Automates ETL processing using ADF pipeline 


 
