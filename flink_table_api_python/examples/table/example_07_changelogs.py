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

from pyflink.table import (TableEnvironment)
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table.expressions import col
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# A table program example that illustrates how to deal with changelogs.
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog("examples")
  env.use_database("marketplace")

  # Table API conceptually views streams as tables. However, every table can also be
  # converted to a stream of changes. This is the so-called 'stream-table duality'.
  # Although you conceptually work with tables, a changelog might be visible when
  # printing to the console for immediate real-time results.

  print("Print an append-only table...")

  # Queries on append-only tables produce insert-only streams.
  # When defining a table based on values, every row in the debug output contains
  # a +I change flag. The flag represents an insert-only change in the changelog
  env.sql_query("VALUES(1), (2), (3), (5), (6)").execute().print()

  print("Print an updating table...")

  # Even if the input was insert-only, the output might be updating. Operations such as
  # aggregations or outer joins might produce updating results with every incoming event.
  # Thus, an updating table becomes an updating stream where -U/+U/-D flags can be observed
  # in the debug output
  env.sql_query("VALUES(1), (2), (3), (5), (6)").alias("c").select(col("c").sum).execute().print()

  # The 'customers' table in the 'examples' catalog is an updating table. It upserts based on
  # the defined primary key 'customer_id'
  customers = env.from_path("customers")

  # Use ConfluentTools to visualise either the changelog or materialized in-memory table.
  # The 'customers' table is unbounded by default, but the tool allows to stop consuming
  # after 100 events for debugging
  print("Print a capped changelog...")
  ConfluentTools.print_changelog_limit(customers, 100)
  print("Print a table of the capped and applied changelog...")
  ConfluentTools.print_materialized_limit(customers, 100)
