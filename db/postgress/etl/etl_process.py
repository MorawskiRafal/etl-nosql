from timeit import default_timer as timer
from pyspark.sql import SparkSession


def process(udf: dict, spark: SparkSession):
    dataframes = dict()
    step_time_info = {
        "data_acquisition_time": -1,
        "etl_processing_time": -1.,
        "overall_time": -1.
    }
    ov_time_start = timer()
    dat_aq_start = ov_time_start

    for udf_val in udf['datasets'].values():
        spark.sql("CLEAR CACHE;")

        dataframes[f"{udf_val['table_schema']}"] = f"./tmp_psql/{udf_val['table_schema']}*"
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://192.168.55.16/") \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", 10000) \
            .option("user", "postgres") \
            .option("password", "") \
            .option("query", udf_val['query']) \
            .load()

        df.createOrReplaceTempView(udf_val['table_schema'])
        if udf['type'] != 'full-pushdown':
            df.write.parquet(f"./tmp_psql/{udf_val['table_schema']}.parquet", mode='overwrite')
        else:
            df.write.format("csv").save(f"postgres/output/{udf['name']}.csv", mode='overwrite')
    dat_aq_end = timer()

    etl_proc_start = timer()
    if udf['type'] != 'full-pushdown':
        spark.sql("CLEAR CACHE;")
        for df_k in dataframes.keys():
            _ = spark.read.option("mergeSchema", "true").parquet(dataframes[df_k])
            _.createOrReplaceTempView(f"{df_k}")

            sqlDF = spark.sql(udf['spark_sql'])
            sqlDF.write.format("csv").save(f"postgres/output/{udf['name']}.csv", mode='overwrite')
        else:
            sqlDF = None

    etl_proc_end = timer()

    spark.sql("CLEAR CACHE;")

    ov_time_end = etl_proc_end

    step_time_info['data_acquisition_time'] = dat_aq_end - dat_aq_start
    step_time_info['etl_processing_time'] = etl_proc_end - etl_proc_start
    step_time_info['overall_time'] = ov_time_end - ov_time_start

    return step_time_info, sqlDF
