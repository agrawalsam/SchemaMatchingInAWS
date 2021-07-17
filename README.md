# SchemaMatchingInAWS
Schema Matching of inbound files to existing Glue Data Catalog Tables to onboard correct files only. 

Whenever a datafile is transferred from one node to another, there is a chance of file being corrupted. 

Various checks need to be performed on the datafile to ensure integrity of data before processing it. 

Schema Matching is one of such process. 

# Usage -
# python schemamatching.py "database" "table" "fileName" 

Here, we are matching schema with Table created in Glue Data Catalog. 
