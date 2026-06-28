# ============================================
# HR Analytics Pipeline - Master Script
# ============================================
# Replace all placeholders before running
# Store credentials in Azure Key Vault in production

from pyspark.sql.functions import col, when, round, count, sum as spark_sum, avg

# ------ CONFIGURATION (replace with your values) ------
client_id = "YOUR_CLIENT_ID"
tenant_id = "YOUR_TENANT_ID"
client_secret = "YOUR_CLIENT_SECRET"
storage_account = "hranalyticsadls"
sql_server = "YOUR_SQL_SERVER.database.windows.net"
sql_database = "hr-analytics-db"
sql_user = "hradmin"
sql_password = "YOUR_SQL_PASSWORD"

# ------ STEP 1: Connect to ADLS2 ------
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
print("ADLS2 connection configured")

# ------ STEP 2: Read raw data ------
df = spark.read.csv(
    f"abfss://hrdata@{storage_account}.dfs.core.windows.net/raw/WA_Fn-UseC_-HR-Employee-Attrition.csv",
    header=True,
    inferSchema=True
)
print(f"Raw data loaded: {df.count()} rows")

# ------ STEP 3: Clean data ------
df_cleaned = df.drop("EmployeeCount", "Over18", "StandardHours")
print(f"Cleaned: {len(df_cleaned.columns)} columns")

# ------ STEP 4: Transform data ------
df_transformed = df_cleaned \
    .withColumn("AttritionFlag",
        when(col("Attrition") == "Yes", 1).otherwise(0)
    ) \
    .withColumn("AgeGroup",
        when(col("Age") < 25, "Under 25")
        .when(col("Age") < 35, "25-34")
        .when(col("Age") < 45, "35-44")
        .when(col("Age") < 55, "45-54")
        .otherwise("55+")
    ) \
    .withColumn("IncomeBand",
        when(col("MonthlyIncome") < 3000, "Low")
        .when(col("MonthlyIncome") < 8000, "Mid")
        .when(col("MonthlyIncome") < 15000, "High")
        .otherwise("Very High")
    ) \
    .withColumn("TenureBand",
        when(col("YearsAtCompany") < 2, "0-2 years")
        .when(col("YearsAtCompany") < 5, "2-5 years")
        .when(col("YearsAtCompany") < 10, "5-10 years")
        .otherwise("10+ years")
    ) \
    .withColumn("AnnualIncome", col("MonthlyIncome") * 12)
print("Transformations applied")

# ------ STEP 5: Build serving tables ------
fact_employee = df_transformed.select(
    "EmployeeNumber", "Age", "AgeGroup", "Gender", "MaritalStatus",
    "Department", "JobRole", "JobLevel", "MonthlyIncome", "AnnualIncome",
    "IncomeBand", "YearsAtCompany", "TenureBand", "TotalWorkingYears",
    "Attrition", "AttritionFlag", "OverTime", "BusinessTravel",
    "JobSatisfaction", "WorkLifeBalance", "EnvironmentSatisfaction",
    "RelationshipSatisfaction", "PerformanceRating"
)
dim_dept_attrition = df_transformed.groupBy("Department").agg(
    count("*").alias("TotalEmployees"),
    spark_sum("AttritionFlag").alias("Attrited"),
    round(spark_sum("AttritionFlag") * 100 / count("*"), 2).alias("AttritionRate")
)
dim_age_attrition = df_transformed.groupBy("AgeGroup").agg(
    count("*").alias("TotalEmployees"),
    spark_sum("AttritionFlag").alias("Attrited"),
    round(spark_sum("AttritionFlag") * 100 / count("*"), 2).alias("AttritionRate")
)
dim_jobrole_income = df_transformed.groupBy("JobRole").agg(
    count("*").alias("TotalEmployees"),
    round(avg("MonthlyIncome"), 2).alias("AvgMonthlyIncome"),
    round(avg("AnnualIncome"), 2).alias("AvgAnnualIncome")
)
print("Serving tables built")

# ------ STEP 6: Write to ADLS2 ------
serving_path = f"abfss://hrdata@{storage_account}.dfs.core.windows.net/serving"
fact_employee.write.mode("overwrite").parquet(f"{serving_path}/fact_employee")
dim_dept_attrition.write.mode("overwrite").parquet(f"{serving_path}/dim_dept_attrition")
dim_age_attrition.write.mode("overwrite").parquet(f"{serving_path}/dim_age_attrition")
dim_jobrole_income.write.mode("overwrite").parquet(f"{serving_path}/dim_jobrole_income")
print("Serving tables written to ADLS2")

# ------ STEP 7: Load into Azure SQL ------
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"
connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
fact_employee.write.mode("overwrite").jdbc(url=jdbc_url, table="fact_employee", properties=connection_properties)
dim_dept_attrition.write.mode("overwrite").jdbc(url=jdbc_url, table="dim_dept_attrition", properties=connection_properties)
dim_age_attrition.write.mode("overwrite").jdbc(url=jdbc_url, table="dim_age_attrition", properties=connection_properties)
dim_jobrole_income.write.mode("overwrite").jdbc(url=jdbc_url, table="dim_jobrole_income", properties=connection_properties)
print("All tables loaded into Azure SQL")
print("Pipeline complete!")
