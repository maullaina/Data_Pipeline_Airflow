# DATA PIPELINES - Project 5 
This is a learning project with the aim to introduce more automation and monitoring to the data warehouse ETL pipelines by applying Apache Airflow. To complete the project, it will be needed to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
<!-- * [License](#license) -->


## General Information
This project is presented in a real case context where Sparkify, a music streaming firm, wants to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Moreover, data quality plays a big part when analyses are executed on top the data warehouse, that is why quality tests are applied after the ETL proces to catch any discrepancies in the datasets.


## Technologies Used
- Python - version 3.0

## Features
List the ready features here:
1. DAGS

A DAG (Directed Acyclic Graph) is the core concept of Airflow, collecting Tasks together, organized with dependencies and relationships to say how they should run. The DAG itself doesn't care about what is happening inside the tasks; it is merely concerned with how to execute them - the order to run them in, how many times to retry them, if they have timeouts, and so on.

2. Plugins

Airflow has a simple plugin manager built-in that can integrate external features to its core by simply dropping files in your $AIRFLOW_HOME/plugins folder.
Airflow offers a generic toolbox for working with data. Different organizations have different stacks and different needs. Using Airflow plugins can be a way for companies to customize their Airflow installation to reflect their ecosystem.
In this project we have 4 different operators:
a)	stage_redshift: to extract data from the s3 to upload it to the Redshift.
b)	load_fact: to generate the fact table songplays.
c)	load_dimension: to generate the 4 dimentional tables arround the fact table. (songs, artists, users and time)
d)	data_quality: a quality test to check if all the ID are not null values. 

3. helpers

Is a script with all the SQL statements that will be used for the operators. 

## Setup
To run this project locally, I wold reccommend to create an environment with "virtual environment" 
Web to create a venv [_here_](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/).

Then, it will be needed to install these packages to make run the project.
- pip install os-sys

Before you start on your first exercise, please note the following instruction. After you have updated the DAG, you will need to run /opt/airflow/start.sh command to start the Airflow webserver.

## Usage

We have applied a ETL pipeline in which there is a connection, using AWS keys, to the public s3 bucket. There, there are all the JSON files. This information is extracted and transformed to a star schema organization. Then, the transformed tbales are uploaded to a data warehouse repository in AWS calles Redshift. 


To run the project it is needed to follow this steps:

`create a Redshift resource and upload the create_tables scripts through the console
Run the Airflow webserver 
Check that the code is the final version
Create the Admin Connections (aws_credentials and redshift_conn
Run the DAG 
Check if the Graph is completed with success)`


## Project Status
Project is: _complete_ 


## Room for Improvement

Examples of queries that we could create 

See if there are preferences on artists between girls and boys: 

> SELECT users.user_id, users.gender, artists.artist_id, artists.name FROM ((users JOIN songplays ON users.user_id = songplay.suser_id) JOIN artists ON songplay.artist_id = artists.artist_id)

## Acknowledgements
- Udacity team.
- colleges from the online course


## Contact
Created by [@maullaina] maullaina@gmail.com