# Building-a-Batch-ETL-Pipeline-RedPanda
Build a batch pipeline to extract data from MySQL, process them with Spark, and load the result into Postgres


In this challenge, you'll create a batch ETL pipeline with Apache Spark that:

Extracts two tables from MySQL database as two Spark data frames.
Join these data frames, and aggregate them to calculate the top selling products.
Write the aggregated result into a Postgres table
Let's get started by setting up the environment.

## Pipeline diagram :
                +------------------------+
                |  Data Sources          |
                |  (New Data, Bitcoin)   |
                +-----------+------------+
                            |
                            v
                +------------------------+
                |    Redpanda            |
                | (Data Ingestion Layer) |
                +-----------+------------+
                            |
                            v
                +------------------------+
                |   QuixStreams          |
                | (Real-Time Processing) |
                +-----------+------------+
                            |
                            v
                +------------------------+
                |  Snowflake Data Marts  |
                +---+---------------+----+
                    |               |
                    v               v
       +-----------------+   +-----------------+
       | Data Mart 1     |   | Data Mart 2     |
       | (New Data)      |   | (Bitcoin Data)  |
       +-----------------+   +-----------------+
                            |
                            v
              +-----------------------------+
              | Final Merged Data (ETL Job) |
              +-----------------------------+


## Logs :
https://my.papertrailapp.com/events

## Redpanda Console :
http://localhost:8080/


## Storage




# References :
@techreport{Docling,
  author = {Deep Search Team},
  month = {8},
  title = {Docling Technical Report},
  url = {https://arxiv.org/abs/2408.09869},
  eprint = {2408.09869},
  doi = {10.48550/arXiv.2408.09869},
  version = {1.0.0},
  year = {2024}
}
