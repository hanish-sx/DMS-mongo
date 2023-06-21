# Databricks notebook source
import os
import pathlib
import datetime
import shutil

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop database qa_mongo_ehrone_prime_bronze_EHRPatientReport CASCADE;
# MAGIC -- CREATE DATABASE IF NOT EXISTS qa_mongo_ehrone_prime_bronze_EHRPatientReport;

# COMMAND ----------

# MAGIC %sh
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/ehrpatientreport
# MAGIC
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/uploadedpatientreport/
# MAGIC
# MAGIC
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/suspendedaccess/
# MAGIC
# MAGIC
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/eHROutputError
# MAGIC
# MAGIC
# MAGIC # ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/
# MAGIC
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/

# COMMAND ----------

# df = spark.read.parquet('/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/eHROutputError/LOAD*')
# display(df)
# df.printSchema()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/

# COMMAND ----------

def table_exists(DATABASE, table_name):
    """Helper function to check if table already exists in a database"""
    existing_tables = spark.catalog.listTables(DATABASE)
    # looks like delta live table names are case insensitive
    return any(t.name == table_name.lower() for t in  existing_tables)

# COMMAND ----------

today = datetime.datetime.now().astimezone(tz=datetime.timezone.utc).strftime('%Y-%m-%d')

CHANGES_LOAD_PATH = '/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta'

BASE_PATH = pathlib.Path(CHANGES_LOAD_PATH)
processing = pathlib.Path(BASE_PATH / 'processing')
processed = pathlib.Path(BASE_PATH / 'processed' / today)

processing.mkdir(parents=True, exist_ok=True)
processed.mkdir(parents=True, exist_ok=True)


def move_files(src, dst):
    # `dir` will be each collection name
    for dir in src.iterdir():
        # `files` will be changes parquet file in each collection
        p = dst / dir.name
        p.mkdir(parents=True, exist_ok=True)
        for file in dir.iterdir():
            print(f'Moving {file!r} to {p!r}')
            try:
                shutil.move(str(file), str(p))
            except shutil.Error:
                # file already exists for some reason
                file.unlink(missing_ok=True)

            
def prepare_files_for_processing(BASE_PATH, processing):
    for dir in [(BASE_PATH / 'EHRPatientReport')]:
        if not dir.exists(): # remove this after test
            continue
        print(f'Moving {dir!r} to {processing!r}')
        # anyway, moving directory seems to take the same amount of time
        # maybe it would be better to move each file ourselves. We can have more control over the copy maybe
        try:
            shutil.move(str(dir), str(processing))
        except OSError as e:
            # likely due to some databricks symlinking files issue ? make sure
            print('error', e)
            # try moving files instead of directories
            move_files(dir, processing / dir.name)


def move_and_clear_processed_data(processing, processed):
    for dir in processing.iterdir():
        print(f'Moving {dir!r} to {processed!r}')
        try:
            shutil.move(str(dir), str(processed))
        except shutil.Error as e:
            print('error clearing', e)
            # dir exists, try moving each files instead
            move_files(dir, processed / dir.name)

# COMMAND ----------

prepare_files_for_processing(BASE_PATH, processing)

# COMMAND ----------

# MAGIC %sh
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processed/2023-04-17/EHRPatientReport/ehrpatientreport | wc -l
# MAGIC # ls -la /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/*
# MAGIC
# MAGIC
# MAGIC ls -la /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/

# COMMAND ----------

# LOAD_PATH = pathlib.Path('/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/')

LOAD_PATH = processing / 'EHRPatientReport'

DATABASE = 'raw'

DELTA_BASE = f'/delta/prod-mongo-dms-EHRPatientReport-{DATABASE}/'

LOAD_PATH, DELTA_BASE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.ehrpatientreport_ehrprime order by uploadTime desc limit 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial load ehrpatientreport

# COMMAND ----------

ehrpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, completedDate: STRUCT<`$date`: BIGINT>, ehr: STRING, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, message: STRING, npi: STRING, numOfRetries: BIGINT, orginalReportId: STRING, patients: ARRAY<STRUCT<_id: STRING, numOfRetries: BIGINT, status: STRING>>, physician: STRING, practice: STRING, priority: STRING, sentToRPATime: STRUCT<`$date`: BIGINT>, singleThreaded: BOOLEAN, status: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadTime: STRUCT<`$date`: BIGINT>, practiceId: BIGINT, completedTime: STRUCT<`$date`: BIGINT>>>'


def initial_load_ehrpatientreport(DATABASE, table_name, delta_path):
    # strip the suffix `_ehrprime` from the table name
    # because we are using the table name to find the parquet files.
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('ehrpatientreport path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = next(path.glob('LOAD*.parquet'), '')
    if not full_load_file:
        print(f'{full_load_file!r} is not found')
        return None # no initial file, which would be weird and never happen for our workflow

    full_load_file = str(pathlib.Path('/') / full_load_file.relative_to('/dbfs'))
    df = spark.read.parquet(full_load_file)
    patient_cols = ['_id', 'numOfRetries', 'status']
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=ehrpatientreport_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('completedTime', F.col('completedTime.$date'))
        .withColumn('patients', F.explode_outer('patients'))
        .select('*', *[F.col(f'patients.{col}').alias(f'patient_{col}') for col in patient_cols])
        .drop('patients')
    )
    display(df)
    #df.printSchema()
    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    print(f'created table {DATABASE!r}.{table_name!r}')
    

table_name = 'ehrpatientreport_ehrprime'
delta_path = os.path.join(DELTA_BASE, table_name)

initial_load_ehrpatientreport(DATABASE, table_name, delta_path)
# select * from raw.ehrpatientreport_ehrprime;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental ehrpatientreport

# COMMAND ----------

ehrpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, completedDate: STRUCT<`$date`: BIGINT>, ehr: STRING, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, message: STRING, npi: STRING, numOfRetries: BIGINT, orginalReportId: STRING, patients: ARRAY<STRUCT<_id: STRING, numOfRetries: BIGINT, status: STRING>>, physician: STRING, practice: STRING, priority: STRING, sentToRPATime: STRUCT<`$date`: BIGINT>, singleThreaded: BOOLEAN, status: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadTime: STRUCT<`$date`: BIGINT>, practiceId: BIGINT, completedTime: STRUCT<`$date`: BIGINT>>'



def incremental_load_ehrpatientreport(DATABASE, table_name):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    
    print('ehrpatientreport path', path, len(incremental_files))
    if len(incremental_files) == 0:
        return None # no incremental files found
    primary_key = '_id'
    secondary_key = 'patient__id'
    changes_df = spark.read.parquet(*incremental_files)
    inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
    changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])


    patient_cols = ['_id', 'numOfRetries', 'status']
    changes_uniq_df = (
        changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema=ehrpatientreport_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('Op', '_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('completedTime', F.col('completedTime.$date'))
        .withColumn('patients', F.explode_outer('patients'))
        .select('*', *[F.col(f'patients.{col}').alias(f'patient_{col}') for col in patient_cols])
        .drop('patients')
    )

    display(changes_uniq_df)
    mapping = {
    #     "Op": "source.Op", # not required
        "_orig_id": "source._orig_id",
        "transact_seq": "source.transact_seq",
        "transact_change_timestamp": "source.transact_change_timestamp",
        "_class": "source._class",
        "_id": "source._id",
        "completedDate": "source.completedDate",
        "ehr": "source.ehr",
        "fromDate": "source.fromDate",
        "location": "source.location",
        "message": "source.message",
        "npi": "source.npi",
        "numOfRetries": "source.numOfRetries",
        "orginalReportId": "source.orginalReportId",
        "physician": "source.physician",
        "practice": "source.practice",
        "priority": "source.priority",
        "sentToRPATime": "source.sentToRPATime",
        "singleThreaded": "source.singleThreaded",
        "status": "source.status",
        "study": "source.study",
        "toDate": "source.toDate",
        "type": "source.type",
        "uploadTime": "source.uploadTime",
        "patient__id": "source.patient__id",
        "patient_numOfRetries": "source.patient_numOfRetries",
        "patient_status": "source.patient_status",
        "practiceId": "source.practiceId",
        "completedTime": "source.completedTime",
    }

    (
         DeltaTable.forName(spark, f'{DATABASE}.{table_name}').alias("target")
             .merge(changes_uniq_df.alias("source"), f"source.{primary_key} = target.{primary_key} and source.{secondary_key} = target.{secondary_key}")
             # # this will work, if there exists a record with the id, which should be there. This may be confusing, when it apprears on the original table, 
             # when you delete the delta changes and do a fresh run again. Because the Insert is removed, and you start with a new delete of the older data in original table
             .whenMatchedDelete("source.Op = 'D'")
             .whenMatchedUpdate(set=mapping)
             .whenNotMatchedInsert(values=mapping)
             .execute()
    )
    

table_name = 'ehrpatientreport_ehrprime'

incremental_load_ehrpatientreport(DATABASE, table_name)

# COMMAND ----------

# %sql
# -- ALTER table raw.ehrpatientreport_ehrprime ADD COLUMNS (practiceId BIGINT, completedTime BIGINT);

# COMMAND ----------

# MAGIC %md
# MAGIC # Hash table for EHR ID mapping

# COMMAND ----------

# import pyspark.sql.functions as F

# from api import get_ehrid

# @F.udf(returnType='string')
# def get_ehr_id_udf(data):
#     report_id = data.id
#     patient_id = data.patient_id
#     return get_ehrid(report_id, patient_id)


# def initial_load_ehrpatientreport_mapping():
#     src_table_name = 'ehrpatientreport_ehrprime'
#     table_name = 'ehrpatientreport_ehrprime_mapping'
#     if table_exists(DATABASE, table_name):
#         print(f'{table_name!r} already exists in database {DATABASE!r}')
#         return None # Nothing to do
#     # spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} AS SELECT _id as id, patient__id as patient_id, cast(null as string) as hashed_ehr_id from {DATABASE}.{src_table_name}")
#     (
#         spark.sql(f"SELECT _id as id, patient__id as patient_id from {DATABASE}.{src_table_name}")
#         .withColumn('ehr_id_hash', F.sha2(get_ehr_id_udf(F.struct('id', 'patient_id')), 256))
#         # .withColumn('ehr_id_hash', F.when(F.col('ehr_id_hash').isNotNull(), F.sha2('ehr_id_hash', 256)).otherwise(F.col('ehr_id_hash')))\ 
#         .write.format('delta').saveAsTable(f'{DATABASE}.{table_name}')
#     )
#     print(f'created table {DATABASE!r}.{table_name!r}')


# def incremental_load_ehrpatientreport_mapping(DATABASE, table_name, target_table_name):
#     path = LOAD_PATH / table_name[:-len('_ehrprime')]
#     incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    
#     print('ehrpatientreport path', path, len(incremental_files))
#     if len(incremental_files) == 0:
#         return None # no incremental files found
#     primary_key = '_id'
#     secondary_key = 'patient__id'
#     changes_df = spark.read.parquet(*incremental_files)
#     inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
#     changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])


#     patient_cols = ['_id', 'numOfRetries', 'status']
#     changes_uniq_df = (
#         changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema=ehrpatientreport_schema))
#         .withColumnRenamed('_id', '_orig_id').drop('_doc').select('Op', '_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
#         .withColumn('patients', F.explode_outer('patients'))
#         .select('*', *[F.col(f'patients.{col}').alias(f'patient_{col}') for col in patient_cols])
#         .drop('patients')
#     ).withColumn('ehr_id_hash', F.sha2(get_ehr_id_udf(F.struct('_id', 'patient__id')), 256))

#     display(changes_uniq_df)
#     mapping = {
#         "id": "source._id",
#         "patient_id": "source.patient__id",
#         "ehr_id_hash": "sha2(source.ehr_id_hash, 256)"
#     }

#     (
#          DeltaTable.forName(spark, f'{DATABASE}.{target_table_name}').alias("target")
#              .merge(changes_uniq_df.alias("source"), f"source.{primary_key} = target.id and source.{secondary_key} = target.patient_id")
#              # # this will work, if there exists a record with the id, which should be there. This may be confusing, when it apprears on the original table, 
#              # when you delete the delta changes and do a fresh run again. Because the Insert is removed, and you start with a new delete of the older data in original table
#              .whenMatchedDelete("source.Op = 'D'")
#              .whenMatchedUpdate(set=mapping)
#              .whenNotMatchedInsert(values=mapping)
#              .execute()
#     )

# src_table_name = 'ehrpatientreport_ehrprime'
# target_table_name = 'ehrpatientreport_ehrprime_mapping'

# # initial_load_ehrpatientreport_mapping() # so that it won't run in tomorrows scheduled run
# # incremental_load_ehrpatientreport_mapping(DATABASE, src_table_name, target_table_name) # only run when the API access is ready
# Uncomment when API is ready.

# COMMAND ----------

# %sql
# -- select * from raw.ehrpatientreport_ehrprime_mapping;
# -- drop table raw.ehrpatientreport_ehrprime_mapping;

# COMMAND ----------

# MAGIC %md
# MAGIC #Initial uploadedpatientreport

# COMMAND ----------

uploadedpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, ehr: STRING, files: ARRAY<STRUCT<fileName: STRING, inputType: STRING>>, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, npi: STRING, physician: STRING, practice: STRING, priority: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadId: STRING, uploadTime: STRUCT<`$date`: BIGINT>>'


def initial_load_uploadedpatientreport(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('uploadedpatientreport path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = next(path.glob('LOAD*.parquet'), '')
    if not full_load_file:
        print(f'{full_load_file!r} is not found')
        return None # no initial file, which would be weird and never happen for our workflow

    full_load_file = str(pathlib.Path('/') / full_load_file.relative_to('/dbfs'))
    df = spark.read.parquet(full_load_file)
    
    file_cols = ['fileName', 'inputType']
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=uploadedpatientreport_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('files', F.explode_outer('files'))
        .select('*', *[F.col(f'files.{col}').alias(f'{col}') for col in file_cols])
        .drop('files')
    )
    display(df)
    #df.printSchema()
    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    print(f'created table {DATABASE!r}.{table_name!r}')


table_name = 'uploadedpatientreport_ehrprime'
delta_path = os.path.join(DELTA_BASE, table_name)


initial_load_uploadedpatientreport(DATABASE, table_name, delta_path)
# select * from raw.uploadedpatientreport;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental uploadedpatientreport

# COMMAND ----------

def incremental_load_uploadedpatientreport(DATABASE, table_name):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    
    print('uploadedpatientreport path', path, len(incremental_files))
    if len(incremental_files) == 0:
        return None # no incremental files found
    
    primary_key = '_id'
    secondary_key = 'fileName' # what if there will two filenames with same _id ?
    changes_df = spark.read.parquet(*incremental_files)
    inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
    changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])
    
    file_cols = ['fileName', 'inputType']
    changes_uniq_df = (
        changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema=uploadedpatientreport_change_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('Op', '_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
    )

    display(changes_uniq_df)
    mapping = {
#         "Op": "source.Op",
        "_orig_id": "source._orig_id",
        "transact_seq": "source.transact_seq",
        "transact_change_timestamp": "source.transact_change_timestamp",
        "_class": "source._class",
        "_id": "source._id",
        "ehr": "source.ehr",
        "fromDate": "source.fromDate",
        "location": "source.location",
        "npi": "source.npi",
        "physician": "source.physician",
        "practice": "source.practice",
        "priority": "source.priority",
        "study": "source.study",
        "toDate": "source.toDate",
        "type": "source.type",
        "uploadId": "source.uploadId",
        "uploadTime": "source.uploadTime",
        "fileName": "source.fileName",
        "inputType": "source.inputType"
    }

    (
         DeltaTable.forName(spark, f'{DATABASE}.{table_name}').alias("target")
             .merge(changes_uniq_df.alias("source"), f"source.{primary_key} = target.{primary_key} and source.{secondary_key} = target.{secondary_key}")
             # # this will work, if there exists a record with the id, which should be there. This may be confusing, when it apprears on the original table, 
             # when you delete the delta changes and do a fresh run again. Because the Insert is removed, and you start with a new delete of the older data in original table
             .whenMatchedDelete("source.Op = 'D'")
             .whenMatchedUpdate(set=mapping)
             .whenNotMatchedInsert(values=mapping)
             .execute()
    )


# there seems to be a change in the schema of initial load and incremental files for this particular collection in the mongodb
uploadedpatientreport_change_schema = 'STRUCT<_class: STRING, _id: STRING, automatedStatus: STRING, dobFormat: STRING, ehr: STRING, fileName: STRING, fromDate: STRUCT<`$date`: BIGINT>, inputType: STRING, isLocked: BOOLEAN, isQueued: BOOLEAN, isSingleThreaded: BOOLEAN, location: STRING, message: STRING, npi: STRING, physician: STRING, physicianSpecialty: STRING, practice: STRING, practiceId: STRING, priority: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadId: STRING, uploadStatus: STRING, uploadTime: STRUCT<`$date`: BIGINT>>'

table_name = 'uploadedpatientreport_ehrprime'

incremental_load_uploadedpatientreport(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?
# select * from raw.uploadedpatientreport;

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial suspendedaccess

# COMMAND ----------

suspendedaccess_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, audit: ARRAY<STRUCT<audit: STRING, date: STRUCT<`$date`: BIGINT>, newValue: STRING, originalValue: STRING, reportId: STRING, user: STRING>>, ehr: STRING, isSuspended: BOOLEAN, practice: STRING, practiceId: STRING>'



def initial_load_suspendedaccess(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('suspendedaccess path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = next(path.glob('LOAD*.parquet'), '')
    if not full_load_file:
        print(f'{full_load_file!r} is not found')
        return None # no initial file, which would be weird and never happen for our workflow

    full_load_file = str(pathlib.Path('/') / full_load_file.relative_to('/dbfs'))
    df = spark.read.parquet(full_load_file)
    
    audit_cols = ['audit', 'date', 'newValue', 'originalValue', 'reportId', 'user']
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=suspendedaccess_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('audit', F.explode_outer('audit'))
        .select('*', *[F.col(f'audit.{col}').alias(f'audit.{col}') for col in audit_cols])
        .drop('audit')
    )
    display(df)
    #df.printSchema()
    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    print(f'created table {DATABASE!r}.{table_name!r}')
    

table_name = 'suspendedaccess_ehrprime'
delta_path = os.path.join(DELTA_BASE, table_name)


initial_load_suspendedaccess(DATABASE, table_name, delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental suspendedaccess

# COMMAND ----------

def incremental_load_suspendedaccess(DATABASE, table_name):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('suspendedaccess path', path)
    incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    
    if len(incremental_files) == 0:
        return # Nothing to do
    primary_key = '_id'
    secondary_key = 'audit.reportId' # what if there will two audit with same _id ?
    changes_df = spark.read.parquet(*incremental_files)
    inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
    changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])
    
    
    audit_cols = ['audit', 'date', 'newValue', 'originalValue', 'reportId', 'user']
    changes_uniq_df = (
        changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema=suspendedaccess_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('Op', '_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('audit', F.explode_outer('audit'))
        .select('*', *[F.col(f'audit.{col}').alias(f'audit.{col}') for col in audit_cols])
        .drop('audit')
    )

    display(changes_uniq_df)
    mapping = {
        "_orig_id": "source._orig_id",
        "transact_seq": "source.transact_seq",
        "transact_change_timestamp": "source.transact_change_timestamp",
        "_class": "source._class",
        "_id": "source._id",
        "ehr": "source.ehr",
        "isSuspended": "source.isSuspended",
        "practice": "source.practice",
        "practiceId": "source.practiceId",
        "`audit.audit`": "source.`audit.audit`",
        "`audit.date`": "source.`audit.date`",
        "`audit.newValue`": "source.`audit.newValue`",
        "`audit.originalValue`": "source.`audit.originalValue`",
        "`audit.reportId`": "source.`audit.reportId`",
        "`audit.user`": "source.`audit.user`"
    }

    (
         DeltaTable.forName(spark, f'{DATABASE}.{table_name}').alias("target")
             .merge(changes_uniq_df.alias("source"), f"source.{primary_key} = target.{primary_key} and source.`{secondary_key}` = target.`{secondary_key}`")
             # # this will work, if there exists a record with the id, which should be there. This may be confusing, when it apprears on the original table, 
             # when you delete the delta changes and do a fresh run again. Because the Insert is removed, and you start with a new delete of the older data in original table
             .whenMatchedDelete("source.Op = 'D'")
             .whenMatchedUpdate(set=mapping)
             .whenNotMatchedInsert(values=mapping)
             .execute()
    )
    

suspendedaccess_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, audit: ARRAY<STRUCT<audit: STRING, date: STRUCT<`$date`: BIGINT>, newValue: STRING, originalValue: STRING, reportId: STRING, user: STRING>>, ehr: STRING, isSuspended: BOOLEAN, practice: STRING, practiceId: STRING>'

table_name = 'suspendedaccess_ehrprime'

incremental_load_suspendedaccess(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial eHROutputError

# COMMAND ----------

eHROutputError_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, errorDate: STRUCT<`$date`: BIGINT>, errorMessage: STRING, fileName: STRING>'


def initial_load_eHROutputError(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('eHROutputError_path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = next(path.glob('LOAD*.parquet'), '')
    if not full_load_file:
        print(f'{full_load_file!r} is not found')
        return None # no initial file, which would be weird and never happen for our workflow

    full_load_file = str(pathlib.Path('/') / full_load_file.relative_to('/dbfs'))
    df = spark.read.parquet(full_load_file)
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=eHROutputError_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
    )
    display(df)
    #df.printSchema()

    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    print(f'created table {DATABASE!r}.{table_name!r}')


table_name = 'eHROutputError_ehrprime'
delta_path = os.path.join(DELTA_BASE, table_name)

initial_load_eHROutputError(DATABASE, table_name, delta_path)
# select * from raw.eHROutputError_ehrprime;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental eHROutputError

# COMMAND ----------

def incremental_load_eHROutputError(DATABASE, table_name):
    path = LOAD_PATH / table_name[:-len('_ehrprime')]
    print('eHROutputError_path', path)
    incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    if len(incremental_files) == 0:
        return # Nothing to do
    primary_key = '_id'
    secondary_key = 'audit' # what if there will two audit with same _id ?
    changes_df = spark.read.parquet(*incremental_files)
    inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
    changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])
    
    changes_uniq_df = (
        changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema=eHROutputError_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('Op', '_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
    )

    display(changes_uniq_df)
    mapping = {
        "_orig_id": "source._orig_id",
        "transact_seq": "source.transact_seq",
        "transact_change_timestamp": "source.transact_change_timestamp",
        "_class": "source._class",
        "_id": "source._id",
        "errorDate": "source.errorDate",
        "errorMessage": "source.errorMessage",
        "fileName": "source.fileName"
    }

    (
         DeltaTable.forName(spark, f'{DATABASE}.{table_name}').alias("target")
             .merge(changes_uniq_df.alias("source"), f"source.{primary_key} = target.{primary_key}")
             # # this will work, if there exists a record with the id, which should be there. This may be confusing, when it apprears on the original table, 
             # when you delete the delta changes and do a fresh run again. Because the Insert is removed, and you start with a new delete of the older data in original table
             .whenMatchedDelete("source.Op = 'D'")
             .whenMatchedUpdate(set=mapping)
             .whenNotMatchedInsert(values=mapping)
             .execute()
    )
    

eHROutputError_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, errorDate: STRUCT<`$date`: BIGINT>, errorMessage: STRING, fileName: STRING>'

table_name = 'eHROutputError_ehrprime'

incremental_load_eHROutputError(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?

# COMMAND ----------

move_and_clear_processed_data(processing, processed)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.eHROutputError_ehrprime limit 5;

# COMMAND ----------

dbutils.notebook.exit('Done')

# COMMAND ----------

1/0
