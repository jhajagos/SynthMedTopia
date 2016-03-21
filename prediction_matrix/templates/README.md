# Overview of the pipeline

This tutorial describes the process of creating, executing and updating maps from relation database tables to the HDF5 file 
matrix format. Between the relational database and the HDF5 format there is a JSON format.
The pipeline can be run at various  points and does not need to be run to the final endpoint. A user may only want to 
map to a JSON document so it can be loaded into a MongoDB instance and another user might start with a JSON document 
and want to generate an HDF5 file.

# Going from relational database to a JSON document

## Mapping a single database table

### Setting up the runtime_config.json 

The `runtime_config.json` file is for setting parameters which will change. In general the runtime_config.json
is not a version controlled file as it may contain passwords to connect to a database. It is divided in 
three sections. Only two sections are mandatory.

```json
{
  "source_db_config": {
    "connection_string": "sqlite:///./test/test_db.db3",
    "limit": null,
    "refresh_transactions_table": 1,
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
  "use_ujson": false,
  "use_gzip_compression": false,
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
to a local SQLite database. The two supported database system are SQLite and PostgreSQL. The 
parameter `"limit"` is used for testing purposes to evaluate mapper output. By setting the
parameter to `null` then there is no limit. The parameter `"refresh_transactions_table"` is set by default to `1` to 
refresh an internal table that is used to join against. The final optional parameter is `"batch_size"` which sets the number
of records that are included in each JSON file.

```json
{
     "base_file_name": "transactions",
     "data_directory": "./test/"
}
```

The `"data_directory"` is the file path. It should be written in a OS specific format, on a Linux system: 
`"/data/analysis/"` or in a windows environment: `"E:\\data\\analysis\"`. The parameter `"base_file_name"` is 
the prefix name for the JSON files, for example, setting it to `"encounters"` will generate 
files `"encounters_1.json"`, `"encounters_2.json"`, ... 

To store the JSON results in a MongoDB instance then the configuration section `"mongo_db_config"`: 
```json
{
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": true
 }
 ```
 
 The parameter `"refresh_collection"` with a value `1` will replace an existing collection.

 For more optimal processing of large number of data there are two additional options. These options make the outputted
 json files less readable. The `"use_ujson"` which is default false is to use the UltraJSON library which is faster than
 the standard JSON library. The final option which saves considerable disk storage space is to use the gzip compression library
 on the generated JSON files.

### Creating a mapping.json file

The mapping file describes how table data gets mapped to a JSON document. The simplest mapping.json
file includes a single mapping rule:

```json
{
    "main_transactions":
        {"table_name": "encounters",
             "fields_to_order_by": ["patient_id", "stay_id", "Discharge Date"],
             "where_criteria": null,
             "transaction_id": "encounter_id",
             "transaction_id_format": "int8",
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

The `"main_transactions"` section specifies details about the base table `"table_name"`.   The `"transaction_id"` parameter
should point to the name primary key of the table. If the transaction_id is not unique than the mapping process will fail. By
default the data type of the transaction_id is assumed to be int8. The parameter `"schema"` sets the database schema.  
For a subset of the table to select the SQL `"where_clause"` can be set. For generating matrices in a specific row order 
the `"fields_to_order_by"` sets this as a list of field names. 

The mapper configuration occurs in the `"mappings"` section.  Each mapping rule is an entry in a list. A mapping rule must have
a `"name"`, `"path"` and a `"type"`. Data is stored in nested dictionaries which creates a path. This is so data can be
 grouped together. By grouping data in paths this helps makes understanding the data clearer. It is general good practice
 to separate your independent variables with the dependent variables when building a predictive model. For example, setting the `"path"` to
 `["independent", "discharge"]` independent variable and for the variables that we are trying to predict 
 `["dependent", "discharge"]`.

The `"type"` parameter supports the following maps: `"one-to-one"`, `"one-to-many"` and `"one-to-many-class"`. The
simplest to start with is `"one-to-one"`.  This pairs a `"transaction_id"` with one and only one row of the target table
 specified by the `"table_name"` parameter. 

## Mapping multiple relational database tables

The `"one-to-many`" maps a relations that is one-to-many. 

## Running the mapper script

# Going from JSON to HDF5

## Mapping a flat document

## Mapping a nested document

## Mapping a list of elements
