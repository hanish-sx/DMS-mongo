# Databricks notebook source
import pathlib
import datetime
import shutil

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop database qa_mongo_ehrone_prime_bronze_EHRPatientReport CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS qa_mongo_ehrone_prime_bronze_EHRPatientReport;

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
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/

# COMMAND ----------

df = spark.read.parquet('/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/EHRPatientReport/eHROutputError/LOAD*')
display(df)
df.printSchema()

# COMMAND ----------

def table_exists(DATABASE, table_name):
    """Helper function to check if table already exists in a database"""
    existing_tables = spark.catalog.listTables(DATABASE)
    return any(t.name == table_name for t in  existing_tables)

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
            shutil.move(str(file), str(p))

            
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

            
prepare_files_for_processing(BASE_PATH, processing)


def move_and_clear_processed_data(processing, processed):
    for dir in processing.iterdir():
        print(f'Moving {dir!r} to {processed!r}')
        try:
            shutil.move(str(dir), str(processed))
        except shutil.Error as e:
            print('error clearing', e)
            # dir exists, try moving each files instead
            move_files(dir, processed / dir.name)
            
            

LOAD_PATH = processing / 'EHRPatientReport'

# COMMAND ----------

#move_and_clear_processed_data(processing, processed) # clean up

# COMMAND ----------

processed
# list(processing.iterdir())

# COMMAND ----------

# MAGIC %sh
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/
# MAGIC 
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/
# MAGIC 
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processed/2023-04-17/EHRPatientReport/ehrpatientreport | wc -l
# MAGIC 
# MAGIC 
# MAGIC ls -la /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/*

# COMMAND ----------

# LOAD_PATH = pathlib.Path('/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/')

LOAD_PATH = processing / 'EHRPatientReport'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
# MAGIC -- table_name = 'ehrpatientreport'
# MAGIC 
# MAGIC select * from qa_mongo_ehrone_prime_bronze_EHRPatientReport.ehrpatientreport order by uploadTime desc limit 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial load ehrpatientreport

# COMMAND ----------

ehrpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, completedDate: STRUCT<`$date`: BIGINT>, ehr: STRING, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, message: STRING, npi: STRING, numOfRetries: BIGINT, orginalReportId: STRING, patients: ARRAY<STRUCT<_id: STRING, numOfRetries: BIGINT, status: STRING>>, physician: STRING, practice: STRING, priority: STRING, sentToRPATime: STRUCT<`$date`: BIGINT>, singleThreaded: BOOLEAN, status: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadTime: STRUCT<`$date`: BIGINT>>'


def initial_load_ehrpatientreport(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name
    print('ehrpatientreport path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = str( next((pathlib.Path('/') / path.relative_to('/dbfs')).glob('LOAD*.parquet'), '') )
    if not full_load_file:
        return None # no initial file, which would be weird and never happen for our workflow
    
    df = spark.read.parquet(full_load_file)
    patient_cols = ['_id', 'numOfRetries', 'status']
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
        .withColumn('patients', F.explode_outer('patients'))
        .select('*', *[F.col(f'patients.{col}').alias(f'patient_{col}') for col in patient_cols])
        .drop('patients')
    )
    display(df)
    #df.printSchema()
    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    
    
DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'ehrpatientreport'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

initial_load_ehrpatientreport(DATABASE, table_name, delta_path)
# select * from qa_mongo_ehrone_prime_bronze_EHRPatientReport.ehrpatientreport;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental ehrpatientreport

# COMMAND ----------

ehrpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, completedDate: STRUCT<`$date`: BIGINT>, ehr: STRING, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, message: STRING, npi: STRING, numOfRetries: BIGINT, orginalReportId: STRING, patients: ARRAY<STRUCT<_id: STRING, numOfRetries: BIGINT, status: STRING>>, physician: STRING, practice: STRING, priority: STRING, sentToRPATime: STRUCT<`$date`: BIGINT>, singleThreaded: BOOLEAN, status: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadTime: STRUCT<`$date`: BIGINT>>'



def incremental_load_ehrpatientreport(DATABASE, table_name):
    path = LOAD_PATH / table_name
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
        "patient_status": "source.patient_status"
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
    

DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'ehrpatientreport'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

incremental_load_ehrpatientreport(DATABASE, table_name)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/ehrpatientreport/
# MAGIC #ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/uploadedpatientreport
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/uploadedpatientreport

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/
# MAGIC mv /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/uploadedpatientreport/ /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/EHRPatientReport/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/processing/

# COMMAND ----------

# MAGIC %md
# MAGIC #Initial uploadedpatientreport

# COMMAND ----------

uploadedpatientreport_schema = 'STRUCT<_class: STRING, _id: STRING, ehr: STRING, files: ARRAY<STRUCT<fileName: STRING, inputType: STRING>>, fromDate: STRUCT<`$date`: BIGINT>, location: STRING, npi: STRING, physician: STRING, practice: STRING, priority: STRING, study: STRING, toDate: STRUCT<`$date`: BIGINT>, type: STRING, uploadId: STRING, uploadTime: STRUCT<`$date`: BIGINT>>'


def initial_load_uploadedpatientreport(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name
    print('uploadedpatientreport path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = str( next((pathlib.Path('/') / path.relative_to('/dbfs')).glob('LOAD*.parquet'), '') )
    if not full_load_file:
        return None # no initial file, which would be weird and never happen for our workflow
    
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
    
DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'uploadedpatientreport'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'


initial_load_uploadedpatientreport(DATABASE, table_name, delta_path)
# select * from qa_mongo_ehrone_prime_bronze_EHRPatientReport.uploadedpatientreport;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental uploadedpatientreport

# COMMAND ----------

def incremental_load_uploadedpatientreport(DATABASE, table_name):
    path = LOAD_PATH / table_name
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


DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'uploadedpatientreport'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

incremental_load_uploadedpatientreport(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?
# select * from qa_mongo_ehrone_prime_bronze_EHRPatientReport.uploadedpatientreport;

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial suspendedaccess

# COMMAND ----------

suspendedaccess_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, audit: ARRAY<STRUCT<audit: STRING, date: STRUCT<`$date`: BIGINT>, newValue: STRING, originalValue: STRING, reportId: STRING, user: STRING>>, ehr: STRING, isSuspended: BOOLEAN, practice: STRING, practiceId: STRING>'



def initial_load_suspendedaccess(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name
    print('suspendedaccess path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = str( next((pathlib.Path('/') / path.relative_to('/dbfs')).glob('LOAD*.parquet'), '') )
    if not full_load_file:
        return None # no initial file, which would be weird and never happen for our workflow
    
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
    
    delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'
    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    
DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'suspendedaccess'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

initial_load_suspendedaccess(DATABASE, table_name, delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental suspendedaccess

# COMMAND ----------

def incremental_load_suspendedaccess(DATABASE, table_name):
    path = LOAD_PATH / table_name
    print('suspendedaccess path', path)
    incremental_files = [str(pathlib.Path('/') / p.relative_to('/dbfs')) for p in path.glob('*.parquet') if not p.name.startswith('LOAD')]
    
    if len(incremental_files) == 0:
        return # Nothing to do
    primary_key = '_id'
    secondary_key = 'audit' # what if there will two audit with same _id ?
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
        "audit.audit": "source.audit.audit",
        "audit.date": "source.audit.date",
        "audit.newValue": "source.audit.newValue",
        "audit.originalValue": "source.audit.originalValue",
        "audit.reportId": "source.audit.reportId",
        "audit.user": "source.audit.user"
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
    

suspendedaccess_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, audit: ARRAY<STRUCT<audit: STRING, date: STRUCT<`$date`: BIGINT>, newValue: STRING, originalValue: STRING, reportId: STRING, user: STRING>>, ehr: STRING, isSuspended: BOOLEAN, practice: STRING, practiceId: STRING>'
    
DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'suspendedaccess'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

incremental_load_suspendedaccess(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial eHROutputError

# COMMAND ----------

eHROutputError_schema = 'STRUCT<_class: STRING, _id: STRUCT<`$oid`: STRING>, errorDate: STRUCT<`$date`: BIGINT>, errorMessage: STRING, fileName: STRING>'


def initial_load_eHROutputError(DATABASE, table_name, delta_path):
    path = LOAD_PATH / table_name
    print('eHROutputError_path', path)
    # create table if not exists
    if table_exists(DATABASE, table_name):
        print(f'{table_name!r} already exists in database {DATABASE!r}')
        return None # Nothing to do
    # since we are using same path for full load and incremental load
    # we will use `LOAD` to distingush between the two for now
    # we'll get the first `LOAD*.parquet` file, and there should only be one
    full_load_file = str( next((pathlib.Path('/') / path.relative_to('/dbfs')).glob('LOAD*.parquet'), '') )
    if not full_load_file:
        return None # no initial file, which would be weird and never happen for our workflow
    
    df = spark.read.parquet(full_load_file)
    df = (
        df.withColumn('doc', F.from_json(df._doc, schema=eHROutputError_schema))
        .withColumnRenamed('_id', '_orig_id').drop('_doc').select('_orig_id', 'transact_seq', 'transact_change_timestamp', 'doc.*')
    )
    display(df)
    #df.printSchema()

    df.write.option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    
DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'eHROutputError'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'
initial_load_eHROutputError(DATABASE, table_name, delta_path)
# select * from qa_mongo_ehrone_prime_bronze_EHRPatientReport.eHROutputError;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental eHROutputError

# COMMAND ----------

def incremental_load_eHROutputError(DATABASE, table_name):
    path = LOAD_PATH / table_name
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

DATABASE = 'qa_mongo_ehrone_prime_bronze_EHRPatientReport'
table_name = 'eHROutputError'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'

incremental_load_eHROutputError(DATABASE, table_name) # the initial and incremental load seems to have different structures. What to do ?

# COMMAND ----------

move_and_clear_processed_data(processing, processed)

# COMMAND ----------

1/0

# COMMAND ----------

# MAGIC %md
# MAGIC # organize, clean up and schedule staging

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrprime-prod-delta/

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

df = spark.read.option("inferSchema", "true").parquet('/mnt/siterx-cdc-dw/ehrone-qa-delta/deIdentifiedIndoDb/deidentifieddocuments/')

display(df)


schema = T.StructType([
    T.StructField("_id", T.StructType([T.StructField("$oid", T.StringType(), True)]), True),
    T.StructField("item", T.StringType(), True),
    T.StructField("qty", T.IntegerType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True),
    T.StructField("size", T.StructType([
        T.StructField("h", T.FloatType(), True),
        T.StructField("w", T.FloatType(), True),
        T.StructField("uom", T.StringType(), True)
    ]), True),
    
])

# display(df.withColumn('doc', F.from_json(df._doc, '_id: struct<$oid: string>, item string, qty int, tags array<string>, size struct<h: int, w: int, uom: string>')))
df = df.withColumn('doc', F.from_json(df._doc, schema))
#display(df.withColumn('doc', F.from_json(df._doc, schema)))
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial load

# COMMAND ----------

DATABASE = 'qa_mongo_ehrone_prime_bronze'
table_name = 'deidentifieddocuments'
delta_path = f'/delta/qa-dms-cdc-{DATABASE}/{table_name}'
#delta_path = f'/delta/test-dms-cdc/{table_name}'
# df.select('_id', 'doc').write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(delta_path)

columns = [F.col(f'doc.{field}').alias(field) for field in ('item', 'qty', 'tags', 'size')]
df.select('_id', *columns).write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from qa_mongo_ehrone_prime_bronze.deidentifieddocuments;
# MAGIC 
# MAGIC -- delete from qa_mongo_ehrone_prime_bronze.deidentifieddocuments where _id is null;
# MAGIC 
# MAGIC -- journal 64364a5940c7d18a8e30b51d

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load

# COMMAND ----------

DATABASE = 'qa_mongo_ehrone_prime_bronze'
table_name = 'deidentifieddocuments'

primary_key = '_id'

changes_df = spark.read.parquet('/mnt/siterx-cdc-dw/ehrone-qa-delta/deIdentifiedIndoDb/deidentifieddocuments/').filter(F.col('transact_seq') != "") # filter for differentiating between full load for now

inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))

changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])


columns = [F.col(f'doc.{field}').alias(field) for field in ('item', 'qty', 'tags', 'size')]
changes_uniq_df = (
    changes_uniq_df.withColumn('doc', F.from_json(changes_uniq_df._doc, schema)) # we need to parse the string as json
    .select('_id', 'Op', *columns) # and then select the required columns
)

display(changes_uniq_df)
# display(changes_df)

mapping = {
    "_id": "source._id",
    "item": "source.item",
    "qty": "source.qty",
    "tags": "source.tags",
    "size": "source.size",
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

# COMMAND ----------

today = datetime.datetime.now().astimezone(tz=datetime.timezone.utc).strftime('%Y-%m-%d')

CHANGES_LOAD_PATH = '/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrone-prod-delta/public/'

BASE_PATH = pathlib.Path(CHANGES_LOAD_PATH)
processing = pathlib.Path(BASE_PATH.parent / 'processing')
procesed = pathlib.Path(BASE_PATH.parent / 'processed' / today)

processing.mkdir(parents=True, exist_ok=True)
procesed.mkdir(parents=True, exist_ok=True)

def prepare_files_for_processing(BASE_PATH, processing):
    for dir in BASE_PATH.iterdir():
        print(dir)
        # anyway, moving directory seems to take the same amount of time
        # maybe it would be better to move each file ourselves. We can have more control over the copy maybe
        # shutil.move(str(dir), str(processing))
        for file in dir.iterdir():
            p = processing / dir.name
            p.mkdir(parents=False, exist_ok=True)
            print(f'Moving {file} to {p}')
            shutil.move(str(file), str(p))

            
prepare_files_for_processing(BASE_PATH, processing)

# COMMAND ----------

DATABASE = 'qa_mongo_ehrone_prime_bronze'

FULL_LOAD_PATH = '/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrone-prod-fullload/public/'
# CHANGES_LOAD_PATH = '/dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrone-prod-delta/public/'
CHANGES_LOAD_PATH = processing


def create_table(table_name):
    # check if table already exists ?
    # can use https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.tableExists.html
    # if version >= 3.3.0
    existing_tables = spark.catalog.listTables(DATABASE)
    if any(t.name == table_name for t in  existing_tables):
        return # table already exists, nothing to do
    
    full_load_path = pathlib.Path(FULL_LOAD_PATH, table_name)
    if not full_load_path.exists():
        return # full load of the table doesn't exist, nothing to do.
    # strip the `/dbfs` prefix, because spark API doesn't need them
    df = spark.read.parquet(str(full_load_path)[len('/dbfs'):])
    #display(encounter_df)
    delta_path = f'/delta/test-dms-cdc-{DATABASE}/{table_name}'
    #delta_path = f'/delta/test-dms-cdc/{table_name}'
    df.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{delta_path}'")
    
    
def update_changes(table_name, primary_key, mapping):
    changes_path = pathlib.Path(CHANGES_LOAD_PATH, table_name)
    if not changes_path.exists():
        return # delta doesn't exist, nothing to do
    # strip the `/dbfs` prefix, because spark API doesn't need them
    changes_df = spark.read.parquet(str(changes_path)[len('/dbfs'):])
    inner_df = changes_df.groupBy(primary_key).agg(F.max('transact_seq').alias('max_seq'))
    changes_uniq_df = changes_df.join(inner_df, (changes_df[primary_key] == inner_df[primary_key]) & (changes_df.transact_seq == inner_df.max_seq), 'inner').drop(changes_df[primary_key])
    display(changes_uniq_df)
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

# COMMAND ----------

email_upsert_mapping = {
    "id": "source.id",
    "name": "source.name",
    "email": "source.email"
}

encounter_upsert_mapping = {
    "enc_date": "source.enc_date", # for some reason `enc_date` is not in initial load ? check why
    "job_uuid": "source.job_uuid",
    "created_date": "source.created_date",
    "updated_at": "source.updated_at",
    "reason": "source.reason",
    "enc_id": "source.enc_id",
    "encounter_filepath": "source.encounter_filepath",
    "qa_check": "source.qa_check",
    "status": "source.status",
    "doc_id": "source.doc_id",
    "npi_id": "source.npi_id"
}

encounter_status_upsert_mapping = {
    "id": "source.id",
    "updated_at": "source.updated_at",
    "enc_id": "source.enc_id",
    "status": "source.status"
}

job_upsert_mapping = {
    "job_uuid": "source.job_uuid",
    "parent_uuid": "source.parent_uuid",
    "created_date": "source.created_date",
    "updated_at": "source.updated_at",
    "schedule_date": "source.schedule_date",
    "config": "source.config",
    "schedule_freq": "source.schedule_freq",
    "job_name": "source.job_name",
    "status": "source.status",
    "emails": "source.emails",
    "job_type": "source.job_type"
}

job_status_upsert_mapping = {
    "id": "source.id",
    "job_uuid": "source.job_uuid",
    "updated_at": "source.updated_at",
    "status": "source.status"
}

patient_upsert_mapping = {
    "created_date": "source.created_date",
    "report_id": "source.report_id",
    "updated_at": "source.updated_at",
    "job_uuid": "source.job_uuid",
    "npi_id": "source.npi_id",
    "reason": "source.reason",
    "status": "source.status",
    "doc_id": "source.doc_id",
    "reason_pretty": "source.reason_pretty",
    "ehr_id": "source.ehr_id",
    "patient_id": "source.patient_id",
    "encounters_path": "source.encounters_path"
}

patient_status_upsert_mapping = {
    "id": "source.id",
    "updated_at": "source.updated_at",
    "doc_id": "source.doc_id",
    "status": "source.status"
}

physicianNPI_upsert_mapping = {
    "id": "source.id",
    "physician_name": "source.physician_name",
    "ehr_system": "source.ehr_system",
    "worker_type": "source.worker_type",
    "practice_name": "source.practice_name",
    "system_name": "source.system_name"
}

report_upsert_mapping = {
    "report_date_end": "source.report_date_end",
    "rx_count": "source.rx_count",
    "icd_count": "source.icd_count",
    "distinct_count": "source.distinct_count",
    "report_id": "source.report_id",
    "created_date": "source.created_date",
    "updated_at": "source.updated_at",
    "job_uuid": "source.job_uuid",
    "study_id": "source.study_id",
    "report_date_start": "source.report_date_start",
    "reason": "source.reason",
    "status": "source.status",
    "report_path_nrm": "source.report_path_nrm",
    "report_path_org": "source.report_path_org",
    "npi_id": "source.npi_id"
}

report_status_upsert_mapping = {
    "id": "source.id",
    "report_id": "source.report_id",
    "updated_at": "source.updated_at",
    "status": "source.status"
}

study_upsert_mapping = {
    "age_high": "source.age_high",
    "age_low": "source.age_low",
    "id": "source.id",
    "extra_criteria": "source.extra_criteria",
    "icd": "source.icd",
    "study_name": "source.study_name",
    "medicine": "source.medicine"
}

systemFolderName_upsert_mapping = {
    "id": "source.id",
    "folder_name": "source.folder_name"
}

toolkit_log_upsert_mapping = {
    "dt": "source.dt",
    "set_num": "source.set_num",
    "id": "source.id",
    "ehrone_job_uuid": "source.ehrone_job_uuid",
    "ext_job_uuid": "source.ext_job_uuid",
    "study_name": "source.study_name",
    "npi": "source.npi",
    "system_name": "source.system_name",
    "set_name": "source.set_name",
    "pt_dir_name": "source.pt_dir_name",
    "uploader_ip": "source.uploader_ip",
    "uploader_hostname": "source.uploader_hostname",
    "status": "source.status",
    "reason": "source.reason",
    "local_filepath": "source.local_filepath",
    "s3_filepath": "source.s3_filepath"
}

toolkit_log_patient_map_upsert_mapping = {
    "id": "source.id",
    "created_dt": "source.created_dt",
    "toolkit_log_id": "source.toolkit_log_id",
    "patient_doc_id": "source.patient_doc_id"
}


table_details = dict(
    email=dict(primary_key='id', mapping=email_upsert_mapping),
    encounter=dict(primary_key='enc_id', mapping=encounter_upsert_mapping),
    encounter_status=dict(primary_key='enc_id', mapping=encounter_status_upsert_mapping),
    job=dict(primary_key='job_uuid', mapping=job_upsert_mapping),
    job_status=dict(primary_key='id', mapping=job_status_upsert_mapping),
    patient=dict(primary_key='doc_id', mapping=patient_upsert_mapping),
    patient_status=dict(primary_key='id', mapping=patient_status_upsert_mapping),
    physicianNPI=dict(primary_key='id', mapping=physicianNPI_upsert_mapping),
    report=dict(primary_key='report_id', mapping=report_upsert_mapping),
    report_status=dict(primary_key='id', mapping=report_status_upsert_mapping),
    study=dict(primary_key='id', mapping=study_upsert_mapping),
    systemFolderName=dict(primary_key='id', mapping=systemFolderName_upsert_mapping),
    toolkit_log=dict(primary_key='id', mapping=toolkit_log_upsert_mapping),
    toolkit_log_patient_map=dict(primary_key='id', mapping=toolkit_log_patient_map_upsert_mapping),
)

for table_name, details in table_details.items():
    primary_key = details['primary_key']
    mapping = details['mapping']
    create_table(table_name)
    update_changes(table_name, primary_key, mapping)


# update_changes('encounter', 'enc_id', encounter_upsert_mapping)
# update_changes('encounter_status', 'id', encounter_status_upsert_mapping)
# update_changes('job', 'job_uuid', job_upsert_mapping)
# update_changes('job_status', 'id', job_status_upsert_mapping)
# update_changes('patient', 'doc_id', patient_upsert_mapping)
# update_changes('patient_status', 'id', patient_status_upsert_mapping)
# update_changes('physicianNPI', 'id', physicianNPI_upsert_mapping)
# update_changes('report', 'report_id', report_upsert_mapping)
# update_changes('report_status', 'id', report_status_upsert_mapping)
# update_changes('study', 'id', study_upsert_mapping)
# update_changes('systemFolderName', 'id', systemFolderName_upsert_mapping)
# update_changes('toolkit_log', 'id', toolkit_log_upsert_mapping)
# update_changes('toolkit_log_patient_map', 'id', toolkit_log_patient_map_upsert_mapping)

# COMMAND ----------

procesed = pathlib.Path(BASE_PATH.parent / 'processed' / today)

def move_and_clear_processed_data(processing, procesed):
    p = pathlib.Path('/tmp') / today
    p.mkdir(parents=True, exist_ok=True)
    for dir in processing.iterdir():
        shutil.copytree(str(dir), str(procesed / dir.name), dirs_exist_ok=True)
    #     shutil.rmtree(dir)
        #shutil.move(str(dir), str(p))
        shutil.rmtree(dir) # probably find a way to move for testing
    
# for dir in processing.iterdir():
#     shutil.rmtree(dir)


# TODO: fix typo of `procesed`
move_and_clear_processed_data(processing, procesed)

# COMMAND ----------

# MAGIC %sh
# MAGIC #for dir in /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrone-prod-delta/processed/2022-11-15/*; do printf "%s " "$dir"; ls "$dir" | wc -l; done
# MAGIC 
# MAGIC for dir in /dbfs/mnt/prod-ehrone-postgres-migration/ehrone_prod/ehrone-prod-delta/processed/**/*; 
# MAGIC do 
# MAGIC   printf "%s " "$dir";
# MAGIC   ls "$dir" | wc -l; 
# MAGIC done
# MAGIC 
# MAGIC #processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from test_prod_ehrone.email where id is not null; -- do a full load and test
# MAGIC -- select * from test_prod_ehrone.email order by id desc;
# MAGIC -- select count(*) from test_prod_ehrone.encounter;
# MAGIC --select count(*) from test_prod_ehrone.patient;
# MAGIC --select count(*) from test_prod_ehrone.job;
# MAGIC 
# MAGIC 
# MAGIC --select * from test_prod_ehrone.patient SORT BY updated_at DESC limit 100;
# MAGIC 
# MAGIC -- select * from test_prod_ehrone.patient where job_uuid='83da1fbd-694b-4846-8221-979fcb8f2168';
# MAGIC select * from prod_ehrone_bronze.patient limit 1;

# COMMAND ----------

dbutils.notebook.exit('Done')

# COMMAND ----------

1/0

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/missing*

# COMMAND ----------

# load missing patients
missing_patient_df = spark.read.parquet('/FileStore/missing_patients.parquet')
display(missing_patient_df.filter("doc_id = '40774-5'"))

# COMMAND ----------

missing_encounter_df = spark.read.parquet('/FileStore/missing_encounters.parquet')
display(missing_encounter_df)

# COMMAND ----------

DATABASE = 'test_prod_ehrone'
table_name = 'patient'
primary_key = 'doc_id'
 
encounter_upsert_mapping = {
    "enc_date": "source.enc_date", # for some reason `enc_date` is not in initial load ? check why
    "job_uuid": "source.job_uuid",
    "created_date": "source.created_date",
    "updated_at": "source.updated_at",
    "reason": "source.reason",
    "enc_id": "source.enc_id",
    "encounter_filepath": "source.encounter_filepath",
    "qa_check": "source.qa_check",
    "status": "source.status",
    "doc_id": "source.doc_id",
    "npi_id": "source.npi_id"
}

patient_upsert_mapping = {
    "created_date": "source.created_date",
    "report_id": "source.report_id",
    "updated_at": "source.updated_at",
    "job_uuid": "source.job_uuid",
    "npi_id": "source.npi_id",
    "reason": "source.reason",
    "status": "source.status",
    "doc_id": "source.doc_id",
    "reason_pretty": "source.reason_pretty",
    "ehr_id": "source.ehr_id",
    "patient_id": "source.patient_id",
    "encounters_path": "source.encounters_path"
}

def upsert_missing(missing_df, mapping, table_name, primary_key):
    (
         DeltaTable.forName(spark, f'{DATABASE}.{table_name}').alias("target")
             .merge(missing_df.alias("source"), f"source.{primary_key} = target.{primary_key}")
             .whenMatchedUpdate(set=mapping)
             .whenNotMatchedInsert(values=mapping)
             .execute()
    )
    
#upsert_missing(missing_patient_df, patient_upsert_mapping, 'patient', 'doc_id')
#upsert_missing(missing_encounter_df, encounter_upsert_mapping, 'encounter', 'enc_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from test_prod_ehrone.patient where doc_id = '40774-5';
# MAGIC --select * from test_prod_ehrone.job where job_uuid = '308f92c0-b8ef-408a-8f06-d37e2c49c20e';
# MAGIC select * from test_prod_ehrone.patient SORT BY updated_at DESC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_prod_ehrone.patient where job_uuid='83da1fbd-694b-4846-8221-979fcb8f2168';

# COMMAND ----------

dbutils.fs.mounts()
