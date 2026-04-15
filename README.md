# DataBase Anomaly Detector

## The problem
>In modern enterprise environments, relational databases—particularly complex systems like Oracle DB—frequently accumulate "dirty data" or structural inconsistencies. Traditional rule-based validation often fails to capture subtle, multi-dimensional anomalies that deviate from historical patterns. These data irregularities do more than just occupy space; they significantly degrade system performance by slowing down complex queries and, more critically, lead to "silent failures" where analytical reports yield erroneous results. For data-driven organizations, undetected anomalies result in flawed business intelligence and increased operational costs, necessitating an automated, intelligent approach to data profiling and integrity.

## A solution
> An ML-powered system that connects to a relational database, profiles its schema and data, and automatically detects anomalies using Isolation Forest and an Autoencoder — designed with Oracle DB compatibility in mind.

## Analysis Scope
Anomaly detection in database systems can be approached through three distinct layers. This project specifically focuses on the **Schema** and **Data** levels, leveraging Machine Learning to identify structural and statistical irregularities.

| Level | Focus | Examples |
| :--- | :--- | :--- |
| **Schema-level** | DB Structure | Type mismatches, redundancy, missing integrity constraints. |
| **Data-level** | Content & Values | Missing values, foreign key violations (orphan records), excessive NULLs, outliers. |
| ~~Performance~~ | ~~Real-time Latency~~ | *Out of scope (requires real-time monitoring tools).* |

> **Note:** We focus on Schema and Data levels because they are best suited for ML-driven pattern recognition. Performance anomalies (latency spikes) typically require real-time infrastructure monitoring rather than static data profiling.

##  Data Strategy: From Kaggle to Oracle
> **Note on Environment:** Since we don't have a real Oracle Database ready to use and to ensure the project is "Enterprise-ready", we simulated a real-world infrastructure by injecting Kaggle's financial data into a local Oracle Database instance using a custom Python-based ETL process.
