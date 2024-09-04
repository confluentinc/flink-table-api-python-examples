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
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# A table program example to interact with catalogs and databases.
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  # Each catalog object is located in a catalog and database
  env.execute_sql("SHOW TABLES IN `examples`.`marketplace`").print()

  # Catalog objects (e.g. tables) can be referenced by 3-part identifiers:
  # catalog.database.object
  env.from_path("`examples`.`marketplace`.`customers`").print_schema()

  # Select a current catalog and database to work with 1-part identifiers.
  # Both Name or ID can be used for this.
  env.execute_sql("SHOW CATALOGS").print()
  env.use_catalog("examples")
  env.execute_sql("SHOW DATABASES").print()
  env.use_database("marketplace")

  # Print the current catalog/database
  print(env.get_current_catalog())
  print(env.get_current_database())

  # Once current catalog/database are set, work with
  # 1-part identifiers (i.e. object names) to read tables
  env.from_path("`customers`").print_schema()
