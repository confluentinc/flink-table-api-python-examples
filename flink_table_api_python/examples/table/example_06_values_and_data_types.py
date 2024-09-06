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
from decimal import Decimal

from pyflink.table import (TableEnvironment)
from pyflink.table.confluent import ConfluentSettings
from pyflink.table.expressions import row, lit, array, map_
from pyflink.table.types import DataTypes
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

#  A table program example to create mock data
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog("examples")
  env.use_database("marketplace")

  # (1) In Python Table API it is encouraged to use SQL expressions
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

  # (2) with Table API expressions
  r = row(
      # BOOLEAN
      lit(True),
      # STRING / CHAR / VARCHAR
      lit("Alice"),
      # DATE
      lit("2018-05-05").to_date,
      # TIME
      lit("3:30:00").to_time,
      # TIMESTAMP
      lit('2016-06-15 3:30:00.001').to_timestamp,
      # BIGINT
      lit(42).cast(DataTypes.BIGINT()),
      # INT
      lit(42),
      # SMALLINT
      lit(42).cast(DataTypes.SMALLINT()),
      # TINYINT
      lit(42).cast(DataTypes.TINYINT()),
      # DOUBLE
      lit(42.0),
      # FLOAT
      lit(42.0).cast(DataTypes.FLOAT()),
      # DECIMAL
      lit(Decimal("123.4567")),
      # BYTES / BINARY / VARBINARY
      lit(bytearray([1, 2, 3])),
      # ARRAY
      array(1, 2, 3),
      # MAP
      map_("k1", "v1", "k2", "v2"),
      # ROW
      row(lit("Bob"), lit(True))
  )

  fromExpressions = env.from_elements([r])

  # Verify the derived data types and values
  print("Table from SQL expressions:")
  fromSql.print_schema()
  fromSql.execute().print()

  print("Table from Table expressions:")
  fromExpressions.print_schema()
  fromExpressions.execute().print()
