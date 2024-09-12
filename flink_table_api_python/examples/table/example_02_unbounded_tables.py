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

from pyflink.table import TableEnvironment
from pyflink.table.confluent import ConfluentSettings
from pyflink.table.expressions import col, row
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# A table program example that illustrates bounded and unbounded statements.
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog("examples")
  env.use_database("marketplace")

  # Statements can be finite (i.e. bounded) or infinite (i.e. unbounded).
  # If one of the accessed input tables is unbounded, the statement is unbounded.

  print("Running bounded statements for listing...")

  # Catalog operations (such as show/list queries) are always finite
  env.execute_sql("SHOW TABLES").print()
  for t in env.list_tables():
    print(t)

  print("Running bounded statement from values...")

  # Pipelines derived from finite tables (such as fromValues) are bounded as well
  env.from_elements([row("Bob"), row("Alice"), row("Peter")]) \
    .alias("name") \
    .filter(col("name").like("%e%")) \
    .execute() \
    .print()

  print("Running unbounded statement...")

  # Confluent's unbounded streaming examples don't terminate and
  # mock real-time data from Kafka
  env.from_path("clicks") \
    .group_by(col("user_id")) \
    .select(col("user_id"), col("view_time").sum) \
    .execute() \
    .print()
