import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.bitcoin_prices (
            timestamp TIMESTAMP PRIMARY KEY,
            google_finance FLOAT,
            coinmarketcap FLOAT
        );
    """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")
    timestamp = kwargs.get('timestamp')
    google_finance = kwargs.get('google_finance')
    coinmarketcap = kwargs.get('coinmarketcap')

    try:
        # Đảm bảo rằng các giá trị là kiểu dữ liệu phù hợp
        session.execute("""
            INSERT INTO spark_streams.bitcoin_prices(timestamp, google_finance, coinmarketcap)
            VALUES (%s, %s, %s)
        """, (timestamp, float(google_finance.replace(',', '')), float(coinmarketcap.replace(',', ''))))  # Chuyển đổi giá trị thành float
        logging.info(f"Data inserted for timestamp {timestamp}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkBitcoinStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.3,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the Spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'bitcoin_prices') \
            .option('startingOffsets', 'earliest') \
            .load()
        print("Kafka DataFrame created successfully")
    except Exception as e:
        print(f"Kafka DataFrame could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(['cassandra'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("google_finance", StringType(), False),
        StructField("coinmarketcap", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    sel = sel.filter(col("timestamp").isNotNull() & 
                     col("google_finance").isNotNull() & 
                     col("coinmarketcap").isNotNull())

    # Chuyển đổi timestamp sang kiểu TimestampType và xóa dấu phẩy trong giá trị tiền tệ
    sel = sel.withColumn("timestamp", col("timestamp").cast(TimestampType())) \
             .withColumn("google_finance", regexp_replace(col("google_finance"), ',', '')) \
             .withColumn("coinmarketcap", regexp_replace(col("coinmarketcap"), ',', ''))

    return sel


if __name__ == "__main__":
    print("Starting the streaming process...")
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        print("Connection to Cassandra established...")
        if session is not None:
            create_keyspace(session)
            create_table(session)

            print("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'bitcoin_prices')
                               .start())

            print("Streaming started...")
            streaming_query.awaitTermination()
