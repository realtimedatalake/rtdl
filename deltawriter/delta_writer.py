import pyspark
from delta import *

from statefun import *
from aiohttp import web

import json
import os

import socket

functions = StatefulFunctions()

# main logic
@functions.bind(typename="com.rtdl.sf.deltawriter/deltawriter")
async def greet(ctx: Context, message: Message):
    host_name = socket.gethostname()
    host_ip = socket.gethostbyname(host_name)
    print("Hostname :  ",host_name)
    print("IP : ",host_ip)
    
    dbname = "rtdl_default_db" # will be populated based on one of project_id/stream_alt_id/stream_id
    tablename = "rtdl_default_table" # will be populated based on one of type/message_type
    data_json = message.raw_value().decode('utf8')
    data = json.loads(data_json) #convert incoming messsage to JSON
    # next we shall populate dbname and tablename
    if "project_id" in data and len(data["project_id"])>0:
        dbname = data["project_id"]
    elif "stream_alt_id" in data and len(data["stream_alt_id"])>0:
        dbname = data["stream_alt_id"]
    elif "stream_id" in data and len(data["stream_id"])>0:
        dbname = data["stream_id"]


    if"type" in data and len(data["type"])>0:
        tablename = data["type"]
    elif "message_type" in data and len(data["message_type"])>0:
        tablename = data["message_type"]

    print(dbname)
    print(tablename)


    # retrieve host and port details for Spark Master
    spark_master_host = os.environ['SPARK_MASTER_HOST']
    if spark_master_host is None or len(spark_master_host)<1:
        spark_master_host = "host.docker.internal"

    spark_master_port = os.environ['SPARK_MASTER_PORT']
    if spark_master_port is None or len(spark_master_port)<1:
        spark_master_port = "7077"

    spark_master_url = "spark://" + spark_master_host + ":" + spark_master_port



    builder = pyspark.sql.SparkSession.builder.appName("RTDL-Spark-Client") \
        .master(spark_master_url) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host",str(host_ip))
        #.config("spark.executor.memory","4g") \
        #.config("spark.driver.memory","4g") \
        #.config("spark.executor.cores",4) \
        
        #.config("spark.driver.bindAddress","0.0.0.0")\
        #.config("spark.driver.host",str(host_ip))\
        #.config("spark.submit.deployMode", "client")\
        #.config("spark.ui.showConsoleProgress", "true")
        
        


    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    jsonDF = spark.read.json(spark.sparkContext.parallelize([data_json]))
    jsonDF.write.format("delta").mode("overwrite").save(dbname + "." + tablename)
    df = spark.read.format("delta").load(dbname + "." + tablename)
    df.show()
    
    


handler = RequestReplyHandler(functions)


async def handle(request):
    req = await request.read()
    res = await handler.handle_async(req)
    return web.Response(body=res, content_type="application/octet-stream")


app = web.Application()
app.add_routes([web.post('/deltawriter', handle)])

if __name__ == '__main__':
    print("DeltaWriter started")
    web.run_app(app, port=8083)

