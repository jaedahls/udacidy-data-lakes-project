Purpose
=================

The purpose of this database is to make Sparkify's music streaming data available for analysis. Through simple queries analysts can understand what songs and artists users are listening to.

Schema and pipeline
=================

The databases uses a star schema, due to its intuitive and time-efficient nature. The pipeline processes song files and log files containing JSON data from Amazon S3 and populates a fact table (song plays) and multiple dimension tables (songs, artists, users and time) in S3 as parquet files.

Explanation of files
=================
* README.md - This file
* etl.py - Initiates the copying of data to staging tables, followed by the insertion of data to final tables.
* dl.cfg - Contains static configuration settings used to access Amazon S3

How to run
================= 
* Run etl.py in ECS cluster to read and populate the final tables.
* Have fun!