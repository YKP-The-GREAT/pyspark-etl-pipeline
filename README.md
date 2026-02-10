# PySpark ETL Pipeline

## Overview

This project demonstrates a PySpark-based ETL pipeline designed to process large datasets using distributed data processing concepts.

## Technologies

* PySpark
* Python
* Docker (optional)
* Linux

## What This Project Demonstrates

* Spark DataFrame transformations and actions
* Partitioning and aggregation
* Handling schema inference and data cleaning
* Writing outputs for downstream analytics

## Context

This project reflects coursework and project-based experience related to scalable data systems and is representative of the type of work I have supported and debugged in academic settings.

## *How to Run*



Windows note
---

On Windows, Spark local mode runs fine for transformations and aggregations, but writing output may require Hadoop `winutils.exe` / `HADOOP\_HOME`.  

For a Windows-friendly run, use:



```bash

python spark\_jobs/etl\_pipeline.py --input data/events.csv --output out/curated\_events --repartition 4 --no\_write



## 

## Quick run (Windows-friendly)

Windows local Spark can require Hadoop `winutils.exe` for filesystem writes. To keep runs reproducible on Windows, use `--no\_write`.



### &nbsp;Generate sample data

```bash

python data/generate\_users.py

python data/generate\_events.py



