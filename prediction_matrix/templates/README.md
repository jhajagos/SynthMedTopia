# Overview of the pipeline

This tutorial describes the process of creating, executing, and updating maps from relation database tables via a JSON 
intermediate format and then to HDF5 file format for machine learning applications. The pipeline can be run at various 
points and does not need to be run to the final endpoint. A user may only want to map to a JSON document so it can be 
loaded into a MongoDB instance and another user might start with a JSON document and want to generate an HDF5 file. 
Also, a single data source maybe mapped differently depending on the problem at hand.

# Going from relational database to a JSON document

## Mapping a single database table

### Setting up the runtime_config.json 

The `runtime_config.json` file is for setting parameters which will change. In general the runtime_config.json
is not a version controlled file as it may contain connection information to a database. It is divided in 
three sections. Only two sections are mandatory.

```json
{
  "source_db_config": {
    "connection_string": "sqlite:///./test/test_db.db3",
    "limit": null,
    "refresh_transactions_table": 1
    "batch_size": 10000
  },
  "json_file_config": {
     "base_file_name": "transactions",
     "data_directory": "./test/"
  },
  "mongo_db_config": {
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": true
  },
  "output_type": "json_file"
}
```

The first section `"source_db_config"` describes the source database which data will be extracted from: 

```json
{
    "connection_string": "sqlite:///./test/test_db.db3",
    "limit": null,
    "refresh_transactions_table": 1,
    "batch_size": 10000
}
```

The `"connection_string"` is a SQLAlchemy formatted [connection string](http://docs.sqlalchemy.org/en/latest/core/engines.html). 
In the example here it is connecting
to a local SQLite database. Only two database are supported at this point and this includes SQLite and PostGRESQL. The 
parameter `"limit"` is used for testing purposes where only a limited number of records processed. By setting the
parameter to `null` then there are no limit. The parameter `"refresh_transactions_table"` is set by default to `1` to 
refresh an internal table that is used to join against. The final optional parameter is `"batch_size"` which sets the number
of records that are included in each file.

```json
{
     "base_file_name": "transactions",
     "data_directory": "./test/"
}
```

The `"data_directory"` is the file path. It should be written in a OS specific format, on a Linux system: 
`"/data/analysis/"` or in a windows environment: `"E:\\data\\analysis\"`. The parameter `"base_file_name"` is 
the prefix name your files will be getting, for example, setting it `"encounters"` will generate 
files `"encounters.json"`.

If you want to store the JSON results in a MongoDB instance then you need to set this configuration section: 
```json
{
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": true
 }
 ```
 
### Creating a mapping.json file

The mapping file describes how your row data maps to a more complicated nested JSON based document. Below is the simplest
mapping JSON file:
```json
{
    "main_transactions":
        {"table_name": "encounters",
             "fields_to_order_by": ["patient_id", "stay_id", "Discharge Date"],
             "where_criteria": null,
             "transaction_id": "encounter_id",
             "schema": null
        },
    "mappings":
    [
        {
            "name": "discharge",
            "path": ["independent", "classes"],
            "table_name": "encounters",
            "type": "one-to-one",
            "fields_to_include":  ["encounter_id",  "medical_record_number",  "drg",
             "patient_gender", "patient_age", "day_from_start"]
        }
    ]
}
```


## Mapping multiple relational database tables to a nested structure

# Going from JSON to HDF5

## Mapping a flat document

## Mapping a nested document

## Mapping a list of elements