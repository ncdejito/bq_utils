from datetime import datetime, timedelta
import json
import logging
import time
import re
import requests
from pathlib import Path

from google.cloud import storage
from google.cloud import bigquery

usd_per_tb = 5

def query(table_name, query, dataset_id, return_size = False, create_view = False, dry_run = False, verbose = True, client = None):
    """
    Read the docs!! https://googleapis.dev/python/bigquery/latest/usage/tables.html
    Add service account by using:
    key_path = "../tkm-project-service-account.json"
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    Args
        return_ (str): if set to 'job_size', will return processed job size in MB. if set to None, returns nothing
    """
    # configure bq
    project, dataset = tuple(dataset_id.split('.'))
    if client is None:
        client = bigquery.Client(project)
    job_config = bigquery.QueryJobConfig()
    if create_view == True:
        # delete view if exists
        table_id = project + '.' + dataset + '.' + table_name + '_view'
        client.delete_table(table_id, not_found_ok=True)
        table_ref = client.dataset(dataset).table(table_name + '_view')
        table = bigquery.Table(table_ref)
        table.view_query = query
        table.view_use_legacy_sql = False
        client.create_table(table)
        if verbose: print("Successfully created view at {}".format(table_name + '_view'));
    if dry_run == True:
        job_config.dry_run = True
        job_config.use_query_cache = False
        query_job = client.query(query, location='US', job_config=job_config)
        # A dry run query completes immediately.
        assert query_job.state == "DONE"
        assert query_job.dry_run
    else:
        table_ref = client.dataset(dataset).table(table_name)
        job_config.destination = table_ref
        # delete table if exists
        table_id = project + '.' + dataset + '.' + table_name
        client.delete_table(table_id, not_found_ok=True)
        # start the query, passing in the extra configuration
        query_job = client.query(query, location='US', job_config=job_config)
        # Waits for the query to finish
        rows = query_job.result()
        if verbose: print('Query results loaded to table {}'.format(table_ref.path));
        table = client.get_table(client.dataset(dataset).table(table_name))
        if verbose: print('Number of rows: {}'.format(table.num_rows));

    job_size = query_job.total_bytes_processed/1024/1024/1024/1024
    if verbose:
        print("This query processes {} MB.".format(job_size*1024*1024));
        print("This query costs {} dollars.".format(job_size*usd_per_tb));
        print()
    if return_size:
        return job_size
    else:
        return None

def run_sql(filename, dataset_id = 'tm-geospatial.cholo_scratch', replace = None, dry_run = False, return_size = False, client = None):
    '''
    Runs a sql file, affecting BQ dataset, replacing parts of the script
    
    Args
        replace (dict): in the format {'str_to_replace': 'replacement'}
    '''
    
    with open(filename) as file:
        script = file.read()
    
    if replace is not None:
        for k, v in replace.items():
            script = script.replace(k, v)
    for snippet in script.split(';'):
        # check first if it's a valid SQL query.
        job_sizes = []
        if re.search('(select)|(from)', snippet, re.IGNORECASE):
            # that should already be the query
            query_ = snippet.strip()
            # get table name
            first_line = query_.split('\n')[0]
            table_name = first_line[first_line.find(': ')+2:]
            # execute query
            job_size = query(
                table_name,
                query_,
                dataset_id,
                return_size = True,
                dry_run = dry_run,
                client = client
            )
            job_sizes.append(job_size)
    if return_size:
        return job_sizes
    else:
        return None
            

def how_much(filename, replace = None):
    job_sizes = run_sql(filename, replace = replace, dry_run = True, return_size = True)
    print("This set of queries costs {} dollars.".format(sum(job_sizes)*usd_per_tb));
