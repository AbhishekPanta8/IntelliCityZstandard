Smart City IoT Data Pipeline
This project implements a Smart City architecture that manages and analyzes IoT data generated from various city sensors, such as traffic lights, air quality monitors, and energy meters. Our big data pipeline consists of Apache NiFi, Apache Kafka, Hadoop HDFS, Apache Spark, and Apache Cassandra, with Zstandard (ZSTD) compression for efficient data storage and transfer.

Table of Contents
Overview
Architecture
Technologies Used
Pipeline Components
Setup Guide
Data Flow
Data Compression with Zstandard
Usage
Future Improvements
Overview
As cities grow, managing resources and ensuring sustainable development becomes increasingly complex. Our Smart City IoT Data Pipeline is designed to handle large volumes of data from IoT sensors throughout the city, helping improve city services, optimize traffic, monitor environmental conditions, and enhance public safety.

Architecture
The architecture supports a scalable, distributed pipeline for real-time data ingestion, processing, and storage:

Data Ingestion: Apache NiFi ingests data from IoT devices and processes it in real-time.
Message Queue: Apache Kafka serves as the messaging layer for high-throughput data transfer.
Data Storage: Hadoop HDFS stores raw, compressed data, while Cassandra stores processed data for fast querying.
Data Processing: Apache Spark performs batch processing and analytics on IoT data.
Compression: Zstandard is used to compress data, reducing storage and bandwidth requirements.
Technologies Used
Apache NiFi: Automates data flow from IoT devices to Kafka.
Apache Kafka: Handles high-throughput, low-latency data streams.
Hadoop HDFS: Provides scalable storage for raw IoT data.
Apache Spark: Processes and analyzes big data.
Apache Cassandra: Enables fast storage and retrieval of structured data.
Zstandard (ZSTD): Offers efficient compression for storage and transmission.
Pipeline Components
Apache NiFi

Role: Collects, processes, and routes IoT data.
Key Processors:
GetFile: Retrieves IoT data from sensor files.
PutKafka: Sends processed data to Kafka topics.
Apache Kafka

Role: Acts as a distributed messaging layer for low-latency data transfer.
Topics:
Raw Data: Stores unprocessed IoT sensor data.
Processed Data: Holds data after initial processing in NiFi.
Hadoop HDFS

Role: Stores raw IoT data in a distributed file system.
Compression: Utilizes Zstandard to optimize storage space.
Apache Spark

Role: Performs data transformations and analytics.
Processing Types:
Batch Processing: Scheduled jobs for data aggregation.
Streaming: Optional real-time processing.
Apache Cassandra

Role: Stores processed IoT data for fast retrieval.
Schema: Designed for querying data by sensor type, location, and timestamp.
Setup Guide
Prerequisites
Java 8 or higher
Docker (optional, for containerized deployment)
Zstandard CLI for compression testing
Steps
Install Apache NiFi and configure data ingestion workflows.
Deploy Apache Kafka and set up topics for raw and processed data.
Install Hadoop and configure HDFS directories.
Set up Apache Spark for batch processing.
Install Apache Cassandra and define schemas.
Enable Zstandard compression on data flows.
Data Flow
Data Ingestion (NiFi): IoT data is ingested and sent to Kafka.
Message Streaming (Kafka): Acts as a buffer for real-time data transfer to HDFS.
Data Storage (HDFS): Kafka streams are stored, compressed using Zstandard.
Data Processing (Spark): Reads data from HDFS, performs analytics, and sends results to Cassandra.
Data Querying (Cassandra): Processed data is accessible for dashboards or applications.
Data Compression with Zstandard
We use Zstandard (ZSTD) for data compression due to its high efficiency. To enable Zstandard:

Configure NiFi and HDFS to utilize .zst compression.
Adjust compression settings based on storage and latency needs.
Usage
Starting the Pipeline
Run NiFi to start data ingestion from IoT sources.
Start Kafka brokers and Spark jobs for processing.
Running Spark Jobs
Use Spark jobs to read from HDFS and transform data.
Example command:
bash
Copy code
spark-submit --class YourSparkJobClass --master yarn your-spark-application.jar
Querying Data in Cassandra
Use CQL commands to retrieve processed data for visualization.
Future Improvements
Real-time Analytics: Integrate Spark Streaming for immediate data processing.
Data Visualization: Add a dashboard (e.g., using Grafana or Tableau) to monitor city metrics.
Machine Learning: Implement predictive analytics for smarter resource management.
