# Usage -
# python schemamatching.py "database" "table" "fileName" 

import pyarrow.parquet as pq
import pandas as pd
import logging
import boto3
import sys

region = "us-east-1" # import region from config

def readFile(fileName: str):
    """
        Read Parquet File at some Name provided.
        Input:
        fileName: pass the Name of file to read schema
        Output:
        Return Schema of that parquet file.
    """
    parquet_schema = pq.read_schema(fileName)
    parquet_schema = pd.DataFrame(({"Column": name, "Type": str(type)} for name, type in zip(parquet_schema.names, parquet_schema.types)))
    parquet_schema = parquet_schema.reindex(columns=["Column", "Type"], fill_value=pd.NA)  # Ensures columns in case the parquet file has an empty dataframe.
    logger.info('Schema of File : %s' % json.dumps(parquet_schema))
    return parquet_schema

def compareSchema(database: str, table: str, fileName: str):
    """
        Connect to the Glue Data Catalog, Get the Table Schema and Match schema with the File at FileName
        Input:
        database: DataBase name in Glue Data Catalog
        table: Table name in Glue Data Catalog
        fileName: pass the Name of file to read schema
        Output:
        If matched, return True
        If not matched, returns False
    """
    try:
        glueClient = boto3.client('glue', region_name=region)
        glueTable = glueClient.get_table(
            DatabaseName=database,
            Name=table
        )
        glueDict = {'Column':[], 'Type':[]}
        for dict in glueTable['Table']['StorageDescriptor']['Columns']:
                glueDict['Column'].append(dict['Name'])
                glueDict['Type'].append(dict['Type'])
        glueDF = pd.DataFrame.from_dict(glueDict)
        logger.info('First 10 Columns & DataTypes : %s' % json.dumps(glueDF.head(10)))
        if glueDF.equals(readFile(fileName)):
            return True
        return False
    except Exception:
        print(traceback.print_exc())

if __name__ == "__main__":
    fileName = sys.argv[1]
    table = sys.argv[2]
    database  = sys.argv[3]
    print(compareSchema(database, table, fileName))