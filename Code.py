from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
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



# Initialize Flask application
app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'chiken123'
jwt = JWTManager(app)

schemas = {
    'employees': ['id', 'name', 'datetime', 'department_id', 'job_id'],
    'departments': ['id', 'department'],
    'jobs': ['id', 'job']
}
tables = list(schemas.keys())
@app.route('/')
def index():
    return 'Welcome to my Flask application!'


@app.route('/login', methods=['POST'])
def login():
    if request.json.get('username') == 'abc' and request.json.get('password') == '123':
        access_token = create_access_token(identity=request.json.get('username'))
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({'message': 'Invalid credentials. Give correct username and password in the request body.'}), 401

# Get table by name
def get_table_by_name(table_name):
    table = None
    if table_name == 'employees':
        table = employees
    elif table_name == 'departments':
        table = departments
    elif table_name == 'jobs':
        table = jobs
    return table

# Function to validate data dictionary rules
def validate_data(row, table_name):
    schema = schemas[table_name]
    skipped_reasons = []

    # Verify same number of fields
    if len(row) != len(schema):
        skipped_reasons.append(f"Number of columns doesn't match schema.")
        return None, skipped_reasons
                    
    # Verify all schema fields are in the row
    for campo in schema:
        if not row.get(campo):
            skipped_reasons.append(f"Missing field {campo}.")
            return None, skipped_reasons
    
    # Convert hire_datetime string to datetime object
    if 'datetime' in schema:
        try:
            row['datetime'] = parse_datetime(row['datetime'])
        except ValueError:
            skipped_reasons.append("Invalid datetime format.")
            return None, skipped_reasons

    # Validate numeric fields
    for campo in ['id', 'department_id', 'job_id']:
        if campo in schema:
            try:
                row[campo] = float(row[campo])
            except ValueError:
                skipped_reasons.append(f"Non-numeric value found in field {campo}.")
                return None, skipped_reasons

    return row, None


# REST API endpoint to receive new data
@app.route('/insert_data/<table_name>', methods=['POST'])
@jwt_required()
def insert_data(table_name):
    if table_name not in tables:
        return jsonify({'error': 'Table not found'}), 404

    data = request.json
    if not isinstance(data, list):
        return jsonify({'error': 'Data must be a list of dictionaries'}), 400

    if len(data) > 1000:
        return jsonify({'error': 'Exceeded maximum allowed rows (1000)'}), 400

    valid_data = []
    skipped_data = {}

    for row in data:
        validated_row, skipped_reasons = validate_data(row, table_name)
        if validated_row:
            valid_data.append(validated_row)
        else:
            skipped_data[str(row)] = skipped_reasons

    if valid_data:
        table = get_table_by_name(table_name)

        try:
            with engine.begin() as conn:
                conn.execute(table.insert(), valid_data)
                conn.commit()  # Commit changes to the database
            return jsonify({'message': 'Data inserted successfully', 'skipped_rows': skipped_data}), 201
        except IntegrityError:
            return jsonify({'error': 'Integrity constraint violation'}), 400
    else:
        return jsonify({'error': 'All rows skipped', 'skipped_rows': skipped_data}), 400


# Backup feature
@app.route('/backup/<table_name>', methods=['POST'])
@jwt_required()
def backup_table(table_name):
    
    if table_name not in tables:
        return jsonify({'error': 'Table not found'}), 404

    data = request.json
    if not isinstance(data, dict):
        return jsonify({'error': 'Data must be a dictionary like {"name": "BackupName"}'}), 400

    backup_name = data.get('name')
    if not backup_name:
        return jsonify({'error': 'Missing or empty "name" field in the request body'}), 400
    
    if not isinstance(backup_name, str):
        return jsonify({'error': 'The value associated with the "name" field must be a string'}), 400
    
    table = get_table_by_name(table_name)

    if table is not None:
        # Get table metadata using SQLAlchemy's inspect function
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)

        # Generate AVRO schema based on table structure
        schema = {
            "type": "record",
            "name": table_name,
            "fields": [
                {"name": col['name'], "type": "string" if col['type'].python_type == str or col['type'].python_type == datetime.datetime else col['type'].python_type.__name__} for col in columns
            ]
        }

        # Retrieve data from the table
        results = session.query(table).all()

        # Convert SQLAlchemy results to list of dictionaries
        data = []
        for row in results:
            row_dict = {}
            for column in table.columns:
                # Convert datetime object to string
                if isinstance(getattr(row, column.name), datetime.datetime):
                    row_dict[column.name] = getattr(row, column.name).strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    row_dict[column.name] = getattr(row, column.name)
            data.append(row_dict)

        # Specify the file path for the backup
        file_path = f'backup/{table_name}_{backup_name}_backup.avro'

        # Write data to AVRO file
        with open(file_path, 'wb') as f:
            fastavro.writer(f, schema, data)

        return jsonify({'message': f'Backup created successfully for table {table_name}', 'file_path': file_path}), 201
    else:
        return jsonify({'error': 'Table not found'}), 404


# Restore feature
@app.route('/restore/<table_name>', methods=['POST'])
@jwt_required()
def restore_table(table_name):
    if table_name not in tables:
        return jsonify({'error': 'Table not found'}), 404

    data = request.json
    if not isinstance(data, dict):
        return jsonify({'error': 'Data must be a dictionary like {"name": "BackupName"}'}), 400

    backup_name = data.get('name')
    if not backup_name:
        return jsonify({'error': 'Missing or empty "name" field in the request body'}), 400
    
    if not isinstance(backup_name, str):
        return jsonify({'error': 'The value associated with the "name" field must be a string'}), 400

    # Specify the file path for the backup
    file_path = f'backup/{table_name}_{backup_name}_backup.avro'

    try:
        with open(file_path, 'rb') as f:
            avro_reader = fastavro.reader(f)
            writer_schema = avro_reader.writer_schema

            # Read data from AVRO file into a list
            avro_data = list(avro_reader)

            # Validate schema against table schema
            table = get_table_by_name(table_name)
            
            expected_columns = [col['name'] for col in writer_schema['fields']]
            actual_columns = [col.name for col in table.columns]
            if expected_columns != actual_columns:
                return jsonify({'error': 'Failed to restore backup. Differences found in the schema of the backup and the table data.'}), 400

            # Convert datetime strings back to datetime objects
            if 'datetime' in expected_columns:
                for row in avro_data:
                    if 'datetime' in row:
                        row['datetime'] = parse_datetime(row['datetime'])
                            
            # Clear existing data in the table
            try:
                with engine.begin() as conn:
                    conn.execute(table.delete())
            except Exception as e:
                return jsonify({'error': f'Failed to restore backup. Could not delete the existing table. Error: {str(e)}'}), 400

            # Insert restored data into the table
            try:
                f.seek(0)
                with engine.begin() as conn:
                    conn.execute(table.insert(), avro_data)
                    conn.commit()  # Commit changes to the database
            except Exception as e:
                return jsonify({'error': f'Failed to restore backup. Could not load avro backup data to the existing table. Error: {str(e)}'}), 400

            return jsonify({'message': f'Data restored successfully for table {table_name} using the backup: {backup_name}'}), 200
    except FileNotFoundError:
        return jsonify({'error': f'Backup avro file not found in path {file_path}.'}), 404
    except Exception as e:
        return jsonify({'error': f'An error occurred while restoring backup: {str(e)}'}), 500


# Challenge 2.1: Get number of employees hired for each job and department in 2021 divided by quarter.
@app.route('/get1', methods=['GET'])
def hired_employees_job_dep_2021_quarter():
    
    try:
        sql_query = text("""
        WITH cte1 as (
            SELECT *, 
            case 
            when cast(strftime('%m', datetime) as integer) between 1 and 3 then 'Q1'
            when cast(strftime('%m', datetime) as integer) between 4 and 6 then 'Q2'
            when cast(strftime('%m', datetime) as integer) between 7 and 9 then 'Q3'
            when cast(strftime('%m', datetime) as integer) between 10 and 12 then 'Q4'
            end as quarter
            FROM employees
            WHERE strftime('%Y', datetime) = '2021'
        ),
        cte2 as (
            SELECT department_id, job_id, quarter, COUNT(*) as count
            FROM cte1
            WHERE quarter is not null
            GROUP BY 1, 2, 3
        )
        SELECT
            d.department,
            j.job,
            SUM(CASE WHEN quarter = 'Q1' THEN count ELSE 0 END) AS Q1,
            SUM(CASE WHEN quarter = 'Q2' THEN count ELSE 0 END) AS Q2,
            SUM(CASE WHEN quarter = 'Q3' THEN count ELSE 0 END) AS Q3,
            SUM(CASE WHEN quarter = 'Q4' THEN count ELSE 0 END) AS Q4
        FROM cte2 a
        LEFT JOIN jobs j ON (a.job_id = j.id)
        LEFT JOIN departments d ON (a.department_id = d.id)
        GROUP BY 1, 2
        ORDER BY 1,2
        """)
        #result = session.execute(sql_query)
        #resultados = result.fetchall()
        with engine.begin() as conn:
            result = conn.execute(sql_query)
            resultados = result.fetchall()

        query_results = []

        for r in resultados:
            row_dict = {column: value for column, value in zip(result.keys(), r)}
            query_results.append(row_dict)

        return jsonify(query_results), 201
    except SQLAlchemyError as e:
        # Handle SQLAlchemy errors
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        # Handle other unexpected errors
        return jsonify({'error': 'Unexpected error occurred'}), 500
    
# Challenge 2.2: List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
@app.route('/get2', methods=['GET'])
def hired_employees_dep_more_than_2021_mean():
    
    try:
        sql_query = text("""
        WITH cte1 as (
            SELECT department_id, COUNT(*) as count
            FROM employees
            WHERE strftime('%Y', datetime) = '2021'
            GROUP BY 1
        ),
        cte2 as (
            SELECT department_id, COUNT(*) as count
            FROM employees
            GROUP BY 1
        )
        SELECT
        a.department_id as id,
        d.department as department,
        a.count as hired
        FROM cte2 a
        LEFT JOIN departments d ON (a.department_id = d.id)
        WHERE a.count > (SELECT AVG(count) FROM cte1)
        """)
        #result = session.execute(sql_query)
        #resultados = result.fetchall()
        with engine.begin() as conn:
            result = conn.execute(sql_query)
            resultados = result.fetchall()

        query_results = []

        for r in resultados:
            row_dict = {column: value for column, value in zip(result.keys(), r)}
            query_results.append(row_dict)

        return jsonify(query_results), 201
    except SQLAlchemyError as e:
        # Handle SQLAlchemy errors
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        # Handle other unexpected errors
        return jsonify({'error': 'Unexpected error occurred'}), 500
    
# Run App
if __name__ == '__main__':
    app.run(debug=False)