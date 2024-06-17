# Pyspark Kafka->Kudu
This pyspark job was create to test the usecase when kafka producer is using Schema Registry. Job is based on Spark Structured Streaming, job is primarly  based on streaming dataframe.

## Algorithm 
1. First there is created streaming DF with apropretiate kafka configs.
2. As second schema registry client is instanced and used for catching the avro schema from SchemaRegistry instance.
3. Schema is cleaned and prepared to the correct avro format.
4. Streaming DF is deserialized with cleaned schema.
5. As last step the whole dataframe is written to the selected target e.g. kudu, console, parquet etc..

## Usage
1. Create Kafka Topic using CLI or SMM.
2. Create Kudu table via impala-shell or HUE.
```sql
CREATE TABLE hriste_hros.spark_schemas ( id INT, firstName STRING, surName STRING, address STRING, phone INT, PRIMARY KEY(id)) PARTITION BY HASH PARTITIONS 16 STORED AS KUDU;
```
3. Clone the repo, modify the required parametes (topics, schemas, table, servers..)
4. get `python-schema-registry-client` module by pip.
```bash
pip install python-schema-registry-client
```
5. package required python modules using pex 
```bash
pip install pex
pex python-schema-registry-client pyspark -o pyspark.pex
```
6. set env variable
```bash
export PYSPARK_PYTHON=./pyspark.pex
```
7. Start the kafka producer e.g. console
8. Run te PySpark job
```bash
sudo -u spark spark3-submit --files pyspark.pex --jars /opt/cloudera/parcels/CDH/lib/kudu/kudu-spark3_2.12.jar spark_schemas.py
```
