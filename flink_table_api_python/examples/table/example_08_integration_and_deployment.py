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

import sys
import uuid
from pyflink.table import (TableEnvironment)
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table.expressions import col, lit, with_all_columns
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

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
# The '%s' parameterizes the SQL for testing.
SQL = "SELECT brand, COUNT(*) AS vendors FROM ProductsMock %s GROUP BY brand"

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
#     poetry run example_08_integration_and_deployment setup
#     poetry run example_08_integration_and_deployment test
#     poetry run example_08_integration_and_deployment deploy
#
# NOTE: The example submits an unbounded background statement. Make sure
# to stop the statement in the Web UI afterward to clean up resources.
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
  """Process command line arguments."""
  if not args:
    args = sys.argv[1:]

  if len(args) == 0:
    print(
        "No mode specified. Possible values are 'setup', 'test', or 'deploy'.")
    exit(1)

  mode = args[0]

  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)
  env.use_catalog(TARGET_CATALOG)
  env.use_database(TARGET_DATABASE)

  if mode == "setup":
    _set_up_program(env)
  elif mode == "test":
    _test_program(env)
  elif mode == "deploy":
    _deploy_program(env)
  else:
    print("Unknown mode: " + mode)
    exit(1)


# --------------------------------------------------------------------------
# Setup Phase
# --------------------------------------------------------------------------
def _set_up_program(env: TableEnvironment):
  print("Running setup...")

  print("Creating table..." + SOURCE_TABLE)
  # Create a mock table that has exactly the same schema as the example
  # `products` table.
  # The LIKE clause is very convenient for this task which is why we use SQL
  # here. Since we use little data, a bucket of 1 is important to satisfy the
  # `scan.bounded.mode` during testing.
  env.execute_sql(
      "CREATE TABLE IF NOT EXISTS `%s`\n"
      "DISTRIBUTED INTO 1 BUCKETS\n"
      "LIKE `examples`.`marketplace`.`products` (EXCLUDING OPTIONS)" %
      SOURCE_TABLE)

  print("Start filling table...")
  # Let Flink copy generated data into the mock table. Note that the
  # statement is unbounded and submitted as a background statement by default.
  pipeline_result = env.from_path("`examples`.`marketplace`.`products`") \
    .select(with_all_columns()) \
    .execute_insert(SOURCE_TABLE)

  print("Waiting for at least 200 elements in table...")
  # We start a second Flink statement for monitoring how the copying progresses
  count_result = env.from_path(SOURCE_TABLE).select(lit(1).count).execute()
  # This waits for the condition to be met:
  with count_result.collect() as results:
    for row in results:
      count = row[0]
      if (count >= 200):
        print("200 elements reached. Stopping...")
        break

  # By using a closable iterator, the foreground statement will be stopped
  # automatically when the iterator is closed. But the background statement
  # still needs a manual stop.
  ConfluentTools.stop_statement(pipeline_result)

  print("Creating table..." + TARGET_TABLE)
  # Create a table for storing the results after deployment.
  env.execute_sql(
      "CREATE TABLE IF NOT EXISTS `%s` \n"
      "(brand STRING, vendors BIGINT, PRIMARY KEY(brand) NOT ENFORCED)\n"
      "DISTRIBUTED INTO 1 BUCKETS" % TARGET_TABLE)


# -----------------------------------------------------------------------------
# Test Phase
# -----------------------------------------------------------------------------
def _test_program(env: TableEnvironment):
  print("Running test...")
  # Dynamic options allow influencing parts of a table scan. In this case, they
  # define a range (from start offset '0' to end offset '100') how to read from
  # Kafka. Effectively, they make the table bounded. If all tables are finite,
  # the statement can terminate. This allows us to run checks on the result.
  dynamicOptions = \
    "/*+ OPTIONS(\n" \
    "'scan.startup.mode' = 'specific-offsets',\n" \
    "'scan.startup.specific-offsets' = 'partition: 0, offset: 0',\n" \
    "'scan.bounded.mode' = 'specific-offsets',\n" \
    "'scan.bounded.specific-offsets' = 'partition: 0, offset: 100'\n" \
    ") */"

  print("Requesting test data...")
  result = env.execute_sql(SQL % dynamicOptions)
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
  statement_name = "vendors-per-brand-" + str(uuid.uuid4())
  env.get_config().set("client.statement-name", statement_name)

  # Execute the SQL without dynamic options.
  # The result is unbounded and piped into the target table.
  result = env.sql_query(SQL % "").execute_insert(TARGET_TABLE)

  # The API might add suffixes to manual statement names such as '-sql' or
  # '-api'. For the final submitted name, use the provided tools.
  finalName = ConfluentTools.get_statement_name(result)

  print("Statement has been deployed as: " + finalName)
