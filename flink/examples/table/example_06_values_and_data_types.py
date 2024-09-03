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

import os

from pyflink.table import (TableEnvironment)
from pyflink.table.confluent import ConfluentSettings

CLOUD_PROPERTIES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     "../../../config/cloud.properties")

#  A table program example to create mock data
if __name__ == '__main__':
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog("examples")
  env.use_database("marketplace")

  # In Python Table API it is encouraged to use SQL expressions
  # since from_elements implementation has many flaws
  fromSql = \
  env.sql_query(
    "VALUES ("
    # VARCHAR(200)
    "CAST('Alice' AS VARCHAR(200)), "
    # BYTES
    "x'010203', "
    # ARRAY
    "ARRAY[1, 2, 3], "
    # MAP
    "MAP['k1', 'v1', 'k2', 'v2', 'k3', 'v3'], "
    # ROW
    "('Bob', true), "
    # NULL
    "CAST(NULL AS INT), "
    # DATE
    "DATE '2024-12-23', "
    # TIME
    "TIME '13:45:59.000', "
    # TIMESTAMP
    "TIMESTAMP '2024-12-23 13:45:59.000', "
    # TIMESTAMP_LTZ
    "TO_TIMESTAMP_LTZ(1734957959000, 3)"
    ")")

  # Verify the derived data types and values
  print("Table from SQL expressions:")
  fromSql.print_schema()
  fromSql.execute().print()