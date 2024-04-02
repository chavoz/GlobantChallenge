# GlobantChallenge
### Author: Nicolás Szoloch
### Version: 1.0 


## Description:
This program, written in Python 3.7, uses a RESTFUL API to receive, backup, restore and analyze data. It uses a SQL Database (SQLALchemy) and a Flask Application to create and Endpoint where users can send HTTP POST and interact with the application.

## Features:
1. Migrate Data: It can load CSV files and load to SQL tables, with integrity and format validaton functions. If the CSV files contain wrong rows, it will skip and print them, so it will only load up the correct data.
2. Insert, Backup and Restore Data: Implemented a RESTFUL API using Flask Application to create different endpoints where user can send their HTTP POST requests for insert data as json, backup data and restore data. 