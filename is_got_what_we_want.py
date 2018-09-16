#!/usr/bin/env python

"""
is_got_what_we_want.py.

This program automates testing accuracy of the trigger parameters each time a change is made to the underlying logic in SQL.
It also outputs any mis-matched records in a data-frame.

Usage: is_got_what_we_want.py [--env=Environment] [--data_set=Data_set]

Options:
    --env=Environment specification. [default: Production]
    --data_set=BigQuery Dataset specification. [default: tests]
"""

from docopt import docopt
from pandas import DataFrame
from util import get_client
from google.cloud import bigquery

def execute_tests(data_set):
    data_ref = client.dataset(data_set)
    tables = list(client.list_tables(data_ref))
    exit_code = 0
    tables_with_want_and_got = False

    for table in tables:
        list_pair = []
        where_clause_construct = []
        result = []
        table_ref = data_ref.table(table.table_id)
        table = client.get_table(table_ref)
        for schema in table.schema:
            if schema.name.startswith("got"):
                got_name = schema.name
                want_name = "want"+schema.name[3:]
                sql_expression = """case when {got} is null and  {want} is not null then false
                                         when {got} is not null and {want} is null then false
                                         when {got} <> {want} then false
                                         else true end""".format(got=got_name,want=want_name)
                list_pair.append(sql_expression+" as {}".format(schema.name[4:]+"_success"))
                where_clause_construct.append(sql_expression+" is false")
        column_specs = ",\n".join(list_pair)
        where_clause = " or\n".join(where_clause_construct)
        sql =  "select TO_JSON_STRING(t) , {}\n\tfrom {}.{} as t where {}".format(column_specs,data_set,table.table_id,where_clause)
        job = client.query(sql,location='US', job_config=bigquery.QueryJobConfig())
        for row in list(job):
            result.append(row)
        if result:
            print("{} has mis-matched columns. Mis-matching results:\n{}".format(table.table_id,DataFrame(result)))
            exit_code = 1
        else:
            exit_code = 0
    if not tables_with_want_and_got:
        print("No tables with want and got columns in this dataset")
    elif exit_code == 0:
            print("Validation complete for all tables in this dataset. No mis-maching columns detected.")
    exit(exit_code)
if __name__ == "__main__":
    opts = docopt(__doc__, version = "Is got what we want: ver 0.1")
    env = opts["--env"]
    data_set = opts["--data_set"]
    if not env:
        env = "production"
    if not data_set:
        data_set = 'tests'
    client = get_client(env,"bigquery")
    execute_tests(data_set)