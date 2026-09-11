################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import uuid
from confluent_pyflink.table import TableEnvironment
from confluent_pyflink.table.utils import ConfluentSettings, ConfluentTools, StatementHandle
from confluent_pyflink.table.expressions import lit, with_all_columns

# NOTE: This example requires write access to a Kafka cluster. Fill out the
# given variables below with target catalog/database if this is fine for you.

# Fill this with an environment you have write access to
TARGET_CATALOG = ""

# Fill this with a Kafka cluster you have write access to
TARGET_DATABASE = ""

# Fill this with names of the Kafka Topics you want to create
SOURCE_TABLE = "ProductsMock"
TARGET_TABLE = "VendorsPerBrand"

# The following SQL will be tested on a finite subset of data before
# it gets deployed to production.
# In production, it will run on unbounded input.
# The '{hints}' field parameterizes the SQL for use during testing.
SQL = "SELECT brand, COUNT(*) AS vendors FROM ProductsMock {hints} GROUP BY brand"


# An example that illustrates how to embed a table program into a CI/CD
# pipeline for continuous testing and rollout.
#
# Because we cannot rely on production data in this example, the program sets
# up some Kafka-backed tables with data during the setup phase.
#
# Afterward, the program can operate in two modes: one for integration testing
# (test phase) and one for deployment (deploy phase).
#
# A CI/CD workflow could execute the following:
#
#     python example_08_integration_and_deployment setup
#     python example_08_integration_and_deployment test
#     python example_08_integration_and_deployment deploy
#
# NOTE: The deploy phase submits an unbounded background statement. Clean it up
# afterward with the operate modes, which manage a statement by name:
#
#     python example_08_integration_and_deployment stop <statement-name>
#     python example_08_integration_and_deployment resume <statement-name>
#     python example_08_integration_and_deployment delete <statement-name>
#
# The complete CI/CD workflow performs the following steps:
#   - Create Kafka table 'ProductsMock' and 'VendorsPerBrand'.
#   - Fill Kafka table 'ProductsMock' with data from marketplace examples table
#     'products'.
#   - Test the given SQL on a subset of data in 'ProductsMock' with the help of
#     dynamic options.
#   - Deploy an unbounded version of the tested SQL that writes into
#     'VendorsPerBrand'.
def run(args=None):
    parser = argparse.ArgumentParser(
        prog="example_08_integration_and_deployment",
        description="CI/CD integration and deployment example.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("setup", help="create tables and fill the source with data")
    sub.add_parser("test", help="run the SQL on bounded data and check the result")
    sub.add_parser("deploy", help="submit the unbounded statement")
    for action in ("stop", "resume", "delete"):
        p = sub.add_parser(action, help=f"{action} a deployed statement by name")
        p.add_argument("statement_name", help=f"name of the statement to {action}")

    parsed_args = parser.parse_args(args)

    settings = ConfluentSettings()
    env = TableEnvironment.create(settings)
    env.use_catalog(TARGET_CATALOG)
    env.use_database(TARGET_DATABASE)

    if parsed_args.mode == "setup":
        _set_up_program(env)
    elif parsed_args.mode == "test":
        _test_program(env)
    elif parsed_args.mode == "deploy":
        _deploy_program(env)
    else:
        _manage_statement(env, parsed_args.mode, parsed_args.statement_name)


# --------------------------------------------------------------------------
# Setup Phase
# --------------------------------------------------------------------------
def _set_up_program(env: TableEnvironment):
    print("Running setup...")

    print(f"Creating table {SOURCE_TABLE}...")
    # Create a mock table that has exactly the same schema as the example
    # `products` table.
    # The LIKE clause is very convenient for this task which is why we use SQL
    # here. Since we use little data, a bucket of 1 is important to satisfy the
    # `scan.bounded.mode` during testing. read-uncommitted makes the freshly filled
    # rows visible immediately, rather than waiting for the exactly-once checkpoint
    # commit.
    env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS `{SOURCE_TABLE}`
        DISTRIBUTED INTO 1 BUCKETS
        WITH ('kafka.consumer.isolation-level' = 'read-uncommitted')
        LIKE `examples`.`marketplace`.`products` (EXCLUDING OPTIONS)
    """)

    print("Start filling table...")
    # Let Flink copy generated data into the mock table. Note that the
    # statement is unbounded and submitted as a background statement by default.
    pipeline_result = (
        env.from_path("`examples`.`marketplace`.`products`")
        .select(with_all_columns())
        .execute_insert(SOURCE_TABLE)
    )

    print("Waiting for at least 200 elements in table...")
    # We start a second Flink statement for monitoring how the copying progresses
    count_result = env.from_path(SOURCE_TABLE).select(lit(1).count).execute()
    # This waits for the condition to be met:
    with count_result.collect() as results:
        for row in results:
            count = row[0]
            if count >= 200:
                print("200 elements reached. Stopping...")
                break

    # By using a closable iterator, the foreground statement will be stopped
    # automatically when the iterator is closed. But the background statement
    # still needs a manual stop.
    ConfluentTools.stop_statement(pipeline_result)

    print(f"Creating table {TARGET_TABLE}...")
    # Create a table for storing the results after deployment.
    env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}`
        (brand STRING, vendors BIGINT, PRIMARY KEY(brand) NOT ENFORCED)
        DISTRIBUTED INTO 1 BUCKETS
    """)


# -----------------------------------------------------------------------------
# Test Phase
# -----------------------------------------------------------------------------
def _test_program(env: TableEnvironment):
    print("Running test...")
    # Dynamic options allow influencing parts of a table scan. In this case, they
    # define a range (from start offset '0' to end offset '100') how to read from
    # Kafka. Effectively, they make the table bounded. If all tables are finite,
    # the statement can terminate. This allows us to run checks on the result.
    dynamicOptions = (
        "/*+ OPTIONS(\n"
        "'scan.startup.mode' = 'specific-offsets',\n"
        "'scan.startup.specific-offsets' = 'partition: 0, offset: 0',\n"
        "'scan.bounded.mode' = 'specific-offsets',\n"
        "'scan.bounded.specific-offsets' = 'partition: 0, offset: 100'\n"
        ") */"
    )

    print("Requesting test data...")
    result = env.execute_sql(SQL.format(hints=dynamicOptions))
    rows = ConfluentTools.collect_materialized(result)

    print("Test data:")
    for row in rows:
        print(row)

    # Use the testing framework of your choice and add checks to verify the
    # correctness of the test data
    testSuccessful = any(r[0] == "Apple" for r in rows)

    if testSuccessful:
        print("Success. Ready for deployment.")
    else:
        print("Test was not successful")
        exit(1)


# ----------------------------------------------------------------------------
# Deploy Phase
# ----------------------------------------------------------------------------
def _deploy_program(env: TableEnvironment):
    print("Running deploy...")

    # It is possible to give a better statement name for deployment but make sure
    # that the name is unique within environment and region.
    statement_name = f"vendors-per-brand-{uuid.uuid4()}"
    ConfluentTools.set_statement_name(env, statement_name)

    # Execute the SQL without dynamic options.
    # The result is unbounded and piped into the target table.
    result = env.sql_query(SQL.format(hints="")).execute_insert(TARGET_TABLE)

    # A handle manages the submitted statement on Confluent Cloud.
    handle = StatementHandle.from_table_result(result)

    # The API might add suffixes to manual statement names such as '-sql' or
    # '-api'. For the final submitted name, use the provided tools.
    print(f"Statement has been deployed as: {handle.get_name()}")

    # Warnings surface non-fatal issues (such as deprecations) that did not stop the
    # statement from being submitted.
    warnings = handle.get_warnings()
    for warning in warnings:
        print(f"  warning [{warning.severity.value}] {warning.reason}: {warning.message}")


# ----------------------------------------------------------------------------
# Operate Phase
# ----------------------------------------------------------------------------
def _manage_statement(env: TableEnvironment, action: str, statement_name: str):
    print(f"Running {action}...")

    handle = StatementHandle.from_name(env, statement_name)

    if action == "stop":
        handle.stop()
    elif action == "resume":
        handle.resume()
    elif action == "delete":
        handle.delete()

    print(f"{action}: {handle.get_name()}")


if __name__ == "__main__":
    run()
