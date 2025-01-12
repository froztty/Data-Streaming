import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

"""This file sets up Spark to process real-time data from Kafka topic
   and transform the data to insert into a Cassandra db. """

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created")

def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                    post_code, email, username, dob, registered_date, phone, picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

# Function to create a Spark connection for processing data
def create_spark_connection():
    # creating spark connection with kafka and1
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created")
    except Exception as e:
        logging.error(f"Couldn't create the spark connection due to exception {e}")

    return s_conn

# creating dataframe for kafka topic
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # initialize Spark API to read streaming data instead of in batches
        # connect to Kafka broker locally
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        if spark_df is not None:
            logging.info("Kafka DataFrame created successfully.")
            logging.info(f"Kafka DataFrame schema: {spark_df.schema}")
        else:
            logging.warning("Kafka DataFrame is None.")

    except Exception as e:
        logging.error(f"Error creating Kafka DataFrame: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])

        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e} ")
        return None

def create_selection_df_from_kafka(spark_df):
    # defining the schema for the Kafka data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # changing to proper format which converts the binary
    # parsing data into json
    # then flatten to have individual columns
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    # Spark session initialized to configure with necessary connectors
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to broker and subscribe to the users_created topic
        spark_df = connect_to_kafka(spark_conn)

        # parse the data as kafka messages are in binary format
        selection_df = create_selection_df_from_kafka(spark_df)

        session = create_cassandra_connection()

        # Insert data into db
        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()