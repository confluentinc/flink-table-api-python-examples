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
from pyflink.table.expressions import row
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# A table program example to get started.
#
# It executes two foreground statements in Confluent Cloud. The results of both
# statements are printed to the console.
def run():
  # Setup connection properties to Confluent Cloud
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)

  # Initialize the session context to get started
  env = TableEnvironment.create(settings)

  print("Running with printing...")

  # The Table API is centered around 'Table' objects which help in defining
  # data pipelines fluently. Pipelines can be defined fully programmatic...
  table = env.from_elements([row("Hello world!")])
  # ... or with embedded Flink SQL
  # table = env.sql_query("SELECT 'Hello world!'")

  # Once the pipeline is defined, execute it on Confluent Cloud.
  # If no target table has been defined, results are streamed back and can be printed
  # locally. This can be useful for development and debugging.
  table.execute().print()

  print("Running with collecting...")

  # Results can not only be printed but also collected locally and accessed
  # individually. This can be useful for testing.
  moreHellos = env.from_elements([
    row("Hello Bob"),
    row("Hello Alice"),
    row("Hello Peter")])
  rows = ConfluentTools.collect_changelog_limit(moreHellos, 10)
  for result in rows:
    print(result[0])
