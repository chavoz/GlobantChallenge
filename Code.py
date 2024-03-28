from flask import Flask, request, jsonify
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import text, inspect
import csv
import os
import datetime
import traceback
import fastavro

# Initialize SQLAlchemy engine
engine = create_engine('sqlite:///database.db', echo=False)
meta = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

# Define database tables
employees = Table(
    'employees', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('datetime', DateTime),
    Column('department_id', Integer),
    Column('job_id', Integer)
)

departments = Table(
    'departments', meta,
    Column('id', Integer, primary_key=True),
    Column('department', String)
)

jobs = Table(
    'jobs', meta,
    Column('id', Integer, primary_key=True),
    Column('job', String)
)

# Drop existing tables and create new ones
meta.drop_all(engine, checkfirst=True)
meta.create_all(engine)

## Migrate initial Data
# Paths to CSV files
csv_files = {
    'hired_employees': ['id', 'name', 'datetime', 'department_id', 'job_id'],
    'departments': ['id', 'department'],
    'jobs': ['id', 'job']
}

# Function to parse datetime string into datetime object
def parse_datetime(datetime_str):
    return datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')

# Function to read CSV files and insert data into the database
def migrate_data():
    for file, schema in csv_files.items():
        with open(file + '.csv', 'r') as csv_file:
            reader = csv.reader(csv_file)
            table_name = file
            table = None
            if table_name == 'hired_employees':
                table = employees
            elif table_name == 'departments':
                table = departments
            elif table_name == 'jobs':
                table = jobs
            if table is not None:  # Check if table exists

                # Parse datetime string if 'datetime' column exists in schema
                if 'datetime' in schema:
                    datetime_index = schema.index('datetime')
                    
                for row in reader:

                    # Validate row against schema format
                    if len(row) != len(schema):
                        print(f"Ignoring row ({table_name}): {row}. Number of columns doesn't match schema.")
                        continue
                        
                    # Convert hire_datetime string to datetime object
                    if 'datetime' in schema:
                        try:
                            row[datetime_index] = parse_datetime(row[datetime_index])
                        except ValueError:
                            print(f"Ignoring row ({table_name}): {row}. Invalid datetime format.")
                            continue

                    # Validate numeric fields
                    for index, value in enumerate(row):
                        if schema[index] in ['id', 'department_id', 'job_id']:  # Skip numeric fields that should be integers
                            try:
                                row[index] = float(value)  # Attempt to convert to float and update value in row
                            except ValueError:
                                print(f"Ignoring row ({table_name}): {row}. Non-numeric value found in field '{schema[index]}'.")
                                break  # Skip inserting this row
                    else:
                        # All validations passed, insert row into database
                        data = {column: value for column, value in zip(schema, row)}
                        session.execute(table.insert().values(**data))

                session.commit()

# Migrate data on application startup
migrate_data()

# Test loaded data with SQL
sql_query = text("SELECT * FROM employees")
result = session.execute(sql_query)
resultados = result.fetchall()

# Print the rows
print("\nHired Employees:")
for r in resultados:
    print(f"ID: {r.id}, Name: {r.name}")


# Test loaded data with SQL 2
sql_query = text("SELECT * FROM jobs")
result = session.execute(sql_query)
resultados = result.fetchall()

# Print the rows
print("\nJobs:")
for r in resultados:
    print(f"ID: {r.id}, Job: {r.job}")