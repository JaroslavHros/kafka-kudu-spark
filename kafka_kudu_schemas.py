"""
Pyspark Streaming Job
    Author:
        @hrosjar 
    Motivation:
        Testing the streaming usecase with schemas 
"""
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
import json, re
#from confluent_kafka.schema_registry import SchemaRegistryClient
from schema_registry.client import SchemaRegistryClient, schema, utils
 
# kafka config
servers = "kafka_servers"
schema_registry = "schema_registry_url"
topic = "Spark"
schema_registry_schema = "Spark"

# kudu config
kudu_master = "kudu_master"
kudu_table = "kudu_table"


# Regular expression to match only required pattern from gained schema
def clean_schema(schema_str):
    corrected = schema_str.replace("'", '"')
    pattern = re.compile(
        r'({"type":\s*"record",\s*"name":\s*"[^"]+",\s*"fields":\s*\[[^\]]*\]})')
    match = pattern.search(corrected)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid schema string")
      
# create json str from cleaned schema   
def create_json(schema):
    try:
        schema_dict = json.loads(schema)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        raise
    # Create a new dictionary with only the valid Avro schema fields
    correct_schema = {
        "type": schema_dict["type"],
        "name": schema_dict["name"],
        "fields": schema_dict["fields"]
    }
    # Convert the dictionary back to a JSON string
    correct_schema_str = json.dumps(correct_schema, indent=2)
    return correct_schema_str

# creating spark context 
spark = SparkSession.builder.appName("testSpark").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# kafka streaming consumer
def spark_consumer():
  df = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers",  servers)\
  .option("subscribe", topic)\
  .option("startingOffsets", "earliest")\
  .option("failOnDataLoss", "false") \
  .load()
  
  # this will print the schema of kafka streaming df
  df.printSchema()
  
  #df.selectExpr("CAST(value AS STRING)")
  # this will work with confluent schema registry, for Cloudera there is no need to use it.
  """df = df.withColumn("magicByte", func.expr("substring(value, 1,1)"))
  df = df.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))
  df = df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))
  df = df.select("magicByte", "valueSchemaId", "fixedValue")
  df.printSchema()"""
  
    # get schema using subject name
  client = SchemaRegistryClient(url=schema_registry)
  sv = client.get_schema('Spark')
  string = str(sv.schema)
  
  # prepare th schema
  cleaned_schema = clean_schema(string)
  string_json = create_json(cleaned_schema)
  
  # deserialize data 
  fromAvroOptions = {"mode":"PERMISSIVE"}
  decoded_output = df.select(from_avro(func.col("value"), string_json, fromAvroOptions).alias("data"))
  # select only the deserialized data by alias
  value_df = decoded_output.select("data.*")
  value_df.printSchema()  # check the schema before printing 
  
  # finally write deserialized kafka stream to the targer (console, parquet, kudu ...)
  """ value_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "true") \
    .start() \
    .awaitTermination() """


 # parse columns to match columns from kudu
  parsed_df = value_df.select( \
    func.col('id').alias('id'), \
    func.col('firstName').alias('firstname'), \
    func.col('surName').alias('surname'), \
    func.col('address').alias('address'), \
    func.col('phone').alias('phone'))
  
  parsed_df.printSchema()

  parsed_df \
    .writeStream \
    .format("org.apache.kudu.spark.kudu") \
    .option("kudu.master", kudu_master) \
    .option("kudu.table", kudu_table) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .start() \
    .awaitTermination() 


""" 
def get_schema(schema_registry_url, schema_name):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version  = sr.get_schema(schema_name)

    return sr, latest_version """

# run the job
spark_consumer()
