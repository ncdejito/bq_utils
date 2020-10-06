# bq_utils
This python script implements 3 functions that wrap common bigquery functions.

```
query('table_name_to_save_as', 'select 1,2,3,4,5')

run_sql('my_queries.sql')

how_much('my_queries.sql')
```

where `my_queries.sql` looks like
```
-- output table: table1
select 1,2,3,4,5;

-- output table: table2
select * from table1;
```
