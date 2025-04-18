# Databricks notebook source
class Gold():
    def __init__(self):
        self.base_data_dir = "/FileStore/covid_processing"
        
    def readSilver(self):
        return spark.readStream.table("cases_line_table")

    def getAggregates(self, cases_line_df):
        from pyspark.sql.functions import avg, expr,sum,when,col
        return (cases_line_df.groupBy("Country_Code","year")
                    .agg(avg("Population").alias("Yearly_Population"),
                         sum(when(col("indicator") == "death",col("daily_count"))).alias("Yearly_deaths"),
                         sum(when(col("indicator") == "confirmed cases",col("daily_count"))).alias("Yearly_cases")
        ))

    def saveResults(self, results_df):
        print(f"\nStarting Gold Stream...", end='')
        return (results_df.writeStream
                    .queryName("gold-update")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/yearly_aggregates")
                    .outputMode("complete")
                    .toTable("yearly_cases")
                )
        print("Done")

    def aggregate(self):
        cases_line_df = self.readSilver()
        aggregate_df = self.getAggregates(cases_line_df)
        sQuery = self.saveResults(aggregate_df)
        return sQuery

# COMMAND ----------

gold_stream = Gold()
gold_SQuery =  gold_stream.process()

# COMMAND ----------

gold_SQuery.stop()

# COMMAND ----------

spark.sql("SELECT * FROM yearly_cases")

# COMMAND ----------

