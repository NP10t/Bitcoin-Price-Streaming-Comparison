# Stock Prices Real-time Comparison

### Personal Project
**September 2024**

## Description
Developed a real-time streaming application that fetches stock prices from two different websites and visualizes them on a comparative chart. This project aims to provide users with an easy-to-understand graphical representation of stock price fluctuations.

## Implementation
Engineered an end-to-end data pipeline using:
- **Apache Airflow** for workflow orchestration.
- **Apache Kafka** for real-time data streaming.
- **Apache Spark** for data processing, storing the data in **Cassandra**.
- Visualizing the data using **Streamlit** for analytics.

## Tech Stack
- **Apache Airflow**
- **Apache Kafka**
- **Apache Spark**
- **Cassandra**
- **Streamlit**

## Diagram
You can view the architecture of this project in the diagram file: [diagram.pdf](diagram.pdf).

## Usage Instructions

### Step 1: Start Docker Compose
1. Make sure you have Docker and Docker Compose installed on your machine.
2. Navigate to the project directory where your `docker-compose.yml` file is located.
3. Run the following command to start all services:
   ```bash
   docker-compose up -d
   ```

### Step 1: Start Apache Airflow
Open your browser and navigate to http://localhost:8080 to access the Airflow UI. Run the DAGs.

### Step 2: Copy the Spark Script to the Spark Container
Use the following command to copy the `spark_stream.py` script into your Spark container:
```bash
docker cp spark_stream.py <spark_container_id>:/opt/bitnami/spark/
```

### Step 3: Submit the Spark Job
Run the Spark job using the command below:

```bash
spark-submit --master local[*] \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
spark_stream.py
```
### Step 4: Access Cassandra
To interact with Cassandra, execute:
```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra
```
You can describe the table for prices with:
```bash
DESCRIBE spark_streams.bitcoin_prices;
```
And to view all records:

```bash
SELECT * FROM spark_streams.bitcoin_prices;
```
### Step 5: Run the Streamlit App
Finally, run the Streamlit application with:
```bash
streamlit run streamlit_app.py
```