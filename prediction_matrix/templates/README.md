# Overview of the pipeline

This tutorial describes the process of creating, executing, and updating maps from relation database tables via a JSON 
intermediate format and then to HDF5 file format for machine learning applications. The pipeline can be run at various 
points and does not need to be run to the final endpoint. A user may only want to map to a JSON document so it can be 
loaded into a MongoDB instance and another user might start with a JSON document and want to generate an HDF5 file. 
Also, a single data source maybe mapped differently depending on the problem at hand.

# Going from relational database to a JSON document

## Mapping a single relational database table

### Setting up the runtime_config.json 

### Creating a mapping.json file

## Mapping multiple tables to a nested document structure

# Going from JSON to HDF5

## Mapping a flat document

## Mapping a nested document

## Mapping a list of elements