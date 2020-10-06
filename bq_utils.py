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

def query(table_name, query, dataset = dataset, create_view = True, dry_run = False, verbose = True, return_ = None):
    """
    Read the docs!! https://googleapis.dev/python/bigquery/latest/usage/tables.html
    
    Args
        return_ (str): if set to 'job_size', will return processed job size in MB. if set to None, returns nothing
    """
    # configure bq
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

    job_size = query_job.total_bytes_processed/1024/1024
    if verbose:
        print("This query will process {} MB.".format(job_size));
        print("This query will process {} dollars.".format(job_size*usd_per_tb));
        print()
    if return is None:
        return None
    else:
        return job_size

def run_sql(filename, dataset = dataset, replace = None, dry_run = False, return_ = None):
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
            query = snippet.strip()
            # get table name
            first_line = query.split('\n')[0]
            table_name = first_line[first_line.find(': ')+2:]
            # execute query
            job_size = query(
                table_name,
                query,
                dataset,
                return_ = return_
            )
            job_sizes.append(job_size)
    if return_ is None:
        return None
    else:
        return job_sizes
            

def how_much(filename):
    job_sizes = run_sql(filename, dry_run = False, return_ = 'job_size')
    print("These set of queries will cost {} dollars.".format(sum(job_sizes)*usd_per_tb));
