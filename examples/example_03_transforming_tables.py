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

from pyflink.table import (TableEnvironment, DataTypes)
from pyflink.table.confluent import ConfluentSettings
from pyflink.table.expressions import col, row, with_all_columns
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# A table program example that demos how to transform data with the Table object.
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog("examples")
  env.use_database("marketplace")

  # The Table API is centered around 'Table' objects. These objects behave similar to SQL
  # views. In other words: You don't mutate data or store it in any way, you only define the
  # pipeline. No execution happens until execute() is called!

  # Read from tables like 'orders'
  orders = env.from_path("orders").select(with_all_columns())

  # Or mock tables with values
  customers = env.from_elements(
      [row(3160, "Bob", "bob@corp.com"),
      row(3107, "Alice", "alice.smith@example.org"),
      row(3248, "Robert", "robert@someinc.com")],
      DataTypes.ROW(
          [DataTypes.FIELD("customer_id", DataTypes.INT()),
          DataTypes.FIELD("name", DataTypes.STRING()),
          DataTypes.FIELD("email", DataTypes.STRING())])
  )

  # Use built-in expressions and functions in transformations such as filter().
  # All top-level expressions can be found in the expressions module;
  # such as col() for selecting columns or lit() for literals.
  # Further expressions can be accessed fluently after the top-level expression;
  # such as in_(), round(), cast(), or alias().
  filteredOrders = orders.filter(col("customer_id").in_(3160, 3107, 3107));

  # add_columns()/rename_columns()/drop_columns() modify some columns while
  # others stay untouched
  transformedOrders = \
  filteredOrders \
  .add_columns(col("price").round(0).cast(DataTypes.INT()).alias("price_rounded")) \
  .rename_columns(col("customer_id").alias("o_customer_id"))

  # Use unbounded joins if the key space is small, otherwise take a look at interval joins
  joinedTable = \
  customers \
  .join(transformedOrders, col("customer_id") == col("o_customer_id")) \
  .drop_columns(col("o_customer_id"))

  # The result shows a joined table with the following columns:
  # customer_id | name | email | order_id | product_id | price | price_rounded
  joinedTable.execute().print()
