import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#Import python modules
from datetime import datetime
 
#Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
 
#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "application_discovery_service_database", table_name = "imdb_movies_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "application_discovery_service_database", table_name = "imdb_movies_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("title", "string", "title", "string"), ("year", "long", "year", "long"), ("budget", "string", "budget", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("title", "string", "title", "string"), ("year", "long", "year", "long"), ("budget", "string", "budget", "double")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://ashish-test-2020/write/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]

data_frame = applymapping1.toDF()

#########################################
### TRANSFORM (MODIFY DATA)
#########################################
 
#Create a decade column from year
decade_col = f.floor(data_frame["year"]/10)*10
data_frame = data_frame.withColumn("decade", decade_col)
 
#Group by decade: Count movies, get average budget
data_frame_aggregated = data_frame.groupby("decade").agg(
f.count(f.col("title")).alias('movie_count'),
f.mean(f.col("budget")).alias('budget_mean'),
)
 
#Sort by the number of movies per the decade
data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("movie_count"))
 
#Print result table
#Note: Show function is an action. Actions force the execution of the data frame plan.
#With big data the slowdown would be significant without cacching.
data_frame_aggregated.show(10)
 
#########################################
### LOAD (WRITE DATA)
#########################################
 
#Create just 1 partition, because there is so little data
data_frame_aggregated = data_frame_aggregated.repartition(1)
 
#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glueContext, "dynamic_frame_write")
 
datasink2 = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_write, connection_type = "s3", connection_options = {"path": "s3://ashish-test-2020/write/"}, format = "csv", transformation_ctx = "datasink2")

job.commit()