import os
import findspark
import time
import random
from pivottablejs import pivot_ui

def init_spark(
        notebook_name = 'beaker_python_notebook',
        initial_executors = 20,
        min_executors = 1,
        max_executors = 40,
        executor_memory = "3G",
        dynamic_allocation = True,
        shuffle_enabled = True,
        shuffle_partitions = 400
    ):
    '''
    Initialize spark context for notebooks
    '''
    findspark.init(os.environ['SPARK_HOME'])
    import pyspark

    return pyspark.sql.SparkSession.builder \
      .master("yarn-client") \
      .config("spark.sql.catalogImplementation", os.getenv('SPARK_CATALOG_IMPLEMENTATION', 'hive')) \
      .config("spark.sql.warehouse.dir", os.environ['SPARK_WAREHOUSE_DIR']) \
      .config("spark.port.maxRetries", "128") \
      .config("spark.app.name", notebook_name) \
      .config("spark.dynamicAllocation.initialExecutors", str(initial_executors)) \
      .config("spark.executor.memory", executor_memory) \
      .config("spark.dynamicAllocation.enabled", str(dynamic_allocation).lower()) \
      .config("spark.dynamicAllocation.minExecutors", str(min_executors)) \
      .config("spark.dynamicAllocation.maxExecutors", str(max_executors)) \
      .config("spark.shuffle.service.enabled", str(shuffle_enabled).lower()) \
      .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
      .getOrCreate()

def display_df_pivot_ui(spark_df):
    '''
    Display dataframe in pivot_ui
    '''
    fname = '%d_%d.html'%(int(time.time()), random.randint(0, 99999999))
    pivot_dir = 'tmp/pivots'
    files_url = '%suser/%s/files'%(os.environ['JUPYTERHUB_BASE_URL'], os.environ['USER'])

    abs_out_path = '/opt/jupyter/notebooks/%s/%s'%(pivot_dir, fname)
    abs_out_url = '%s/%s/%s'%(files_url, pivot_dir, fname)

    return pivot_ui(spark_df.toPandas(), outfile_path=abs_out_path, url=abs_out_url)
