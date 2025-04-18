# Databricks notebook source
from pyspark.sql.functions import col
class Silver():
    def __init__(self):
        self.base_data_dir = '/FileStore/covid_processing'

    def readCases(self):
        return spark.readStream .option("skipChangeCommits", "true").table("total_cases")
    

    def flatten(self, cases_df):
        return cases_df.selectExpr("value.country as Country","value.country_code as Country_code","value.continent as Continent","value.population as Population","value.indicator as Indicator","value.daily_count as Daily_count","value.case_date as date", "year('date') as year")

    def clean(self, flat_df):
        return flat_df.na.drop(subset=["Country_code"])

    def appendCases(self,final_df):
        return (final_df.writeStream.format("delta").option("checkpointLocation",f"{self.base_data_dir}/checkpoint/cases_line_table").queryName("silver-processing")
                .outputMode("append").toTable("cases_line_table"))
        
    def transform(self):
        print(f"Starting silver stream .....")
        cases_df = self.readCases()
        print("Reading message ...")
        flat_df  = self.flatten(cases_df)
        print("Flattening df ...")
        final_df = self.clean(flat_df)
        print("Removing bad data types")
        streamQ = self. appendCases(final_df)
        print("Saved to table")
        return streamQ

# COMMAND ----------

silver_stream = Silver()
silver_SQuery = silver_stream.process()

# COMMAND ----------

silver_SQuery.stop()

# COMMAND ----------

dbutils.fs.rm("/FileStore/covid_processing/checkpoint/cases_line_table", recurse=True)

# COMMAND ----------

