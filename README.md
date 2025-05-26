#  Fraud detection in Bangking Transictions usging Hadoop

This project demonstrates a real-time and batch-layer solution for detecting fraudulent credit card transactions using the Hadoop ecosystem, combining data from AWS RDS, Kafka, and local flat files. It applies multiple rule-based detection techniques to identify suspicious transactions.


##  Input Datasets

| Dataset Name        | Description                                                | Source       |
|---------------------|------------------------------------------------------------|--------------|
| `card_member`       | Card and member metadata (card ID, member ID, joining date)| AWS RDS      |
| `member_score`      | Member credit score information                            | AWS RDS      |
| `card_transactions` | Historical transactions (amount, date, status, location)   | Flat file    |
| `Kafka Stream`      | Real-time transactions from POS terminals                  | Kafka topic  |


##  System Architecture & Data Pipeline

### 1. Ingest Batch Data from AWS RDS Using Sqoop

- Import `card_member` and `member_score` tables into HDFS using Sqoop.
- Configure incremental jobs with:
  - `--incremental append` or `--incremental lastmodified`
  - `--check-column` and `--last-value`
- Store the imported data in ORC or Parquet format for efficient Hive querying.

### 2. Load Historical Transactions from CSV into HBase via Hive

- Copy `card_transactions.csv` from local to HDFS using `hdfs dfs -put`.
- Create a Hive external table over this file.
- Create an HBase table with composite row key: `card_id + transaction_dt`.
- Use Hive-HBase integration to populate HBase using:
  ```sql
  CREATE EXTERNAL TABLE hbase_card_transactions(...)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES (...)
  TBLPROPERTIES ("hbase.table.name" = "card_transactions");


* Insert transaction data into the HBase table from Hive.



##  Fraud Detection Rules

###  Rule 1: Upper Control Limit (UCL)

**Objective:** Detect high-amount outliers based on recent transaction history.

**Formula:**
`UCL = Moving Avg (Last 10 Genuine Txns) + 3 × Std Dev`

**Implementation Steps:**

* Use Hive window functions to extract last 10 genuine transactions for each card.
* Compute moving average and standard deviation.
* Store UCL values in a Hive table and update HBase for quick access.



###  Rule 2: Credit Score Rule

**Objective:** Flag users with poor credit history.

**Logic:**

* If `score < 200` → Mark as **Fraudulent**

**Implementation Steps:**

* Credit scores are updated every 4 hours using incremental Sqoop import.
* Lookup table in HBase is updated for real-time access during streaming analysis.



###  Rule 3: Zip Code Distance vs. Time

**Objective:** Identify geographically impossible transactions.

**Logic:**

* Calculate speed = distance / time between current and previous transaction.
* Use provided postcode distance library.
* If speed exceeds humanly possible limits → Mark as **Fraudulent**

**Implementation Steps:**

* Lookup table stores `last_postcode` and `last_transaction_dt` for each card.
* On new transactions, retrieve previous data from HBase, compute speed.
* If transaction is genuine, update HBase with new postcode and timestamp.


##  Real-Time Stream Processing with Spark & Kafka

### 4. Real-Time Transaction Ingestion

* Use Spark Streaming to subscribe to the Kafka topic for live POS transactions.
* Parse payload: `card_id`, `member_id`, `amount`, `pos_id`, `postcode`, `transaction_dt`.

### 5. Real-Time Rule Application

For each incoming transaction:

* Apply **Rule 1** by comparing amount to UCL from HBase.
* Apply **Rule 2** by checking credit score from HBase.
* Apply **Rule 3** using postcode distance library and HBase history.

**Result:**

* If any rule is violated → **Fraudulent**
* Else → **Genuine**



##  Output Actions

* Write the classified result (Genuine or Fraudulent) back to Hive or HBase.
* For **Genuine** transactions, update zip code and timestamp in HBase.



##  Technologies Used

| Component            | Tool/Service                     |
| -------------------- | -------------------------------- |
| Batch Ingestion      | Apache Sqoop                     |
| Data Storage         | HDFS, Apache Hive, Apache HBase  |
| Stream Ingestion     | Apache Kafka                     |
| Stream Processing    | Apache Spark Streaming           |
| Distance Calculation | Custom Postcode Distance Library |



##  Summary

This end-to-end pipeline leverages Hadoop's ecosystem to:

* Efficiently ingest and query historical and real-time transaction data.
* Apply rule-based logic to detect fraudulent activity.
* Deliver near real-time fraud detection at scale using Spark and Kafka.
* Ensure data consistency and fast lookups using Hive-HBase integration.
