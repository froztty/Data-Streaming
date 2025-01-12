# Realtime Data Streaming Project

This project demonstrates how to stream real-time data from a Kafka topic into Apache Spark, process the data, and store it in a Cassandra database. 
The purpose of this project is to learn how to handle real-time data streaming and processing by building an end to end data pipeline while making use of the different technological tools. 
Technology used: **Apach Airflow**, **PostgreSQL** **Kafka**, **Apache Spark**, **Cassandra**, and **Docker** 

---

## Architecture

- **Kafka**: Kafka is used as the real-time message broker to stream the data. In this case, the topic `users_created` is where data about users is published.
- **Spark**: Apache Spark reads data from the Kafka topic, processes the data in real-time, and writes the transformed data to Cassandra.
- **Cassandra**: The data is stored in Cassandra for later retrieval and querying.

---

## Prerequisites

Make sure you have the following installed:

- **Docker**: For creating and running the containerized services (Kafka, Spark, Cassandra).
- **Docker Compose**: For managing multi-container Docker applications.
- **Python 3.11**: For running the Spark streaming script (`spark_streaming.py`).

---

## Getting Started

Follow the steps below to set up and run the project.

### 1. Clone the Repository

```bash
git clone https://github.com/froztty/Data-Streaming.git
cd <project_directory>
```

### 2. Set up the Docker Containers

Make sure Docker and Docker Compose are installed.
Start the Docker containers (might have to run twice):

```bash
docker compose up -d
```

This will start the following services:

- **Zookeeper**: Required for Kafka to function.
- **Kafka Broker**: The Kafka message broker.
- **Spark Master and Worker**: Apache Spark setup for streaming processing.
- **Cassandra**: NoSQL database to store the processed data.
- **Schema Registry**: Manages the schema of Kafka messages.
- **Control Center**: Provides a UI to monitor Kafka and topics.
- **Apache Airflow Webserver**: Used to trigger DAGs as admin

### 3. Install Python Dependencies

You can install the required Python dependencies by running:

```bash
pip install -r requirements.txt
```

### 4. Run the Streaming Script

The core functionality is implemented in the `spark_streaming.py` script. You can run the script to start streaming and processing the data.

```bash
python spark_streaming.py
```

### 5. Verify the Data in Cassandra

After running the script, you can connect to Cassandra and check that the data has been inserted into the `created_users` table.

You can use the following CQL command to check the data:

```bash
docker exec -it cassandra cqlsh
```

Once inside the Cassandra shell, you can query the data:

```sql
USE spark_streams;
SELECT * FROM created_users;
```

---

## Future Improvements

- Expand the project to support more complex data processing pipelines.
- Integrate with other data storage systems like relational databases (also switching between NoSQL databases like MongoDB).
- Set up monitoring and alerting for Kafka, Spark, and Cassandra.
- Add different types of data from other APIs (creating possibly unstructured data without schemas)
