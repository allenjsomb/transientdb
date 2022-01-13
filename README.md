# TransientDB
Lightweight database to provide quick access to data using SQL.
Use it whenever you want an easy solution for standing up a sql database for storing and retrieving data. 

## Configuration
### config.ini
```
[server]
tag = default
listen = 0.0.0.0
port = 8000
access_log = False
debug = False
auto_reload = False
schemas_folder = /transientdb/schemas
data_folder = /transientdb/data

[auth]
token = bec2f8b3969f054523bd06dbb736c3cb67446ecbdbd2e99987e270a53cc29cc7

[dump]
enabled = True
interval_minutes = 30
record_count = 10000

```

## Endpoints
### POST /execute
```
Execute SQL where return value is not expected (e.g. DELETE )
```
### GET /count/{table:str}
```
Return count of records in the database table.
```
### POST /index/{table:str}/{name:str}/{field:str}
```
Create an index on table called some name on a field or column of the table.
```
### POST /query
```
Execute SQL where return value is expected (e.g. SELECT )
```
### GET /tables
```
Get list of tables in the database.
```
### GET /{table:str}?{col1}={value1}[,{col2}={value2}},...]
```
Query a table in the database by specific columns and values.
```
### GET /vacuum
```
Perform vacuum operation on the database.
```