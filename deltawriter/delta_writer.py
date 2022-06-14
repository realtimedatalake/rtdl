import pyspark
from delta import *
from delta import pip_utils
from delta.tables import *

from statefun import *
from aiohttp import web

import json
import os

import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

configs = [] #collection of configs
functions = StatefulFunctions()

# main logic
@functions.bind(typename="com.rtdl.sf/deltawriter")
async def greet(ctx: Context, message: Message):

    host_name = socket.gethostname()

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
        if data["message_type"] == "rtdl_205" : #ignore control messages
            configs.clear()
            for filename in os.listdir("configs"):
                f = os.path.join("configs", filename)
                # checking if it is a file
                if os.path.isfile(f):
                    configs.append(json.load(open(f)))
            print(len(configs),' configs loaded')
            return




    # retrieve host and port details for Spark Master
    try:
        spark_master_host = os.environ['SPARK_MASTER_HOST']
        if spark_master_host is None or len(spark_master_host)<1:
            spark_master_host = "0.0.0.0"
    except:
        spark_master_host = "0.0.0.0" #localhost also if key not specified

    try:
        spark_master_port = os.environ['SPARK_MASTER_PORT']
        if spark_master_port is None or len(spark_master_port)<1:
            spark_master_port = "7077"
    except:
        spark_master_port = "7077" # default port if not specified

    spark_master_url = "spark://" + spark_master_host + ":" + spark_master_port

    builder = pyspark.sql.SparkSession.builder.appName("RTDL-Spark-Client") \
        .master(spark_master_url) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .config("spark.jars.ivy","/tmp/.ivy") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

    spark = pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()
    
    jsonDF = spark.read.json(spark.sparkContext.parallelize([data_json]))
    
    try:
        file_store_root = os.environ['FILE_STORE_ROOT']
    except:
        file_store_root = None # if not defined, no need to add

    table_path = ""
    if not file_store_root is None and len(file_store_root)>0:
        table_path += file_store_root + "/"
    table_path += dbname + "/" + tablename
    #print(table_path)
    try:
        jsonDF.write.format("delta").mode("append").save(table_path)
    except:
        pass # will need to log and pass to next function
    df = spark.read.format("delta").load(table_path)
    df.show()
    print("spark part done")
    #now for dynamic routing to next function
    #first identify the matching config
    matching_config = {}
    for config in configs:
        if "project_id" in data and len(data["project_id"])>0 and "project_id" in config and len(config["project_id"])>0 and data["project_id"]==config["project_id"]:
            matching_config = config
            break
        elif "stream_alt_id" in data and len(data["stream_alt_id"])>0 and "stream_alt_id" in config and len(config["stream_alt_id"])>0 and data["stream_alt_id"]==config["stream_alt_id"]:
            matching_config = config
            break
        elif "stream_id" in data and len(data["stream_id"])>0 and "stream_id" in config and len(config["stream_id"])>0 and data["stream_id"]==config["stream_id"]:
            matching_config = config
            break
    
    
    #check if there's a list of functions in the matching config
    if "functions" in matching_config:
        functions = matching_config["functions"].split(",")
        deltawriter_index = functions.index("deltawriter")
        if len(functions)>deltawriter_index+1: #more elements after deltawriter
            topic_name = functions[deltawriter_index+1]+"-ingress"
            '''
            ctx.send_ingress(kafka_ingress_message(
                typename='com.rtdl.sf/' + functions[deltawriter_index+1], 
                topic=topic_name, 
                key="message",
                value=byte(data_json,'utf-8')))
            '''
            kafka_url = os.environ['KAFKA_URL']
            if os.environ == '':
                print("KAFKA_URL should not be empty")
            else:
                producer = KafkaProducer(bootstrap_servers=kafka_url,max_block_ms=1000)
                future = producer.send(topic_name,key=bytes('message','utf-8'),value=json.dumps(data).encode('utf-8'))
                try:
                    result = future.get(timeout=10)
                    producer.close()
                    print("egress message written")
                except KafkaError:
                    log.exception()
                    pass

handler = RequestReplyHandler(functions)

async def handle(request):
    req = await request.read()
    res = await handler.handle_async(req)
    return web.Response(body=res, content_type="application/octet-stream")


app = web.Application()
app.add_routes([web.post('/deltawriter', handle)])

if __name__ == '__main__':
    #first load all configs into memory
    for filename in os.listdir("configs"):
        f = os.path.join("configs", filename)
        # checking if it is a file
        if os.path.isfile(f):
            configs.append(json.load(open(f)))
    print(len(configs),' configs loaded')
    print("DeltaWriter started")
    web.run_app(app, port=8083)
