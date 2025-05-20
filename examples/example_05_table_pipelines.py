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

from pyflink.table import (TableEnvironment, DataTypes, Schema)
from pyflink.table.confluent import ConfluentSettings, ConfluentTableDescriptor
from pyflink.table.expressions import col, row, concat, lit
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH


# NOTE: This example requires write access to a Kafka cluster. Fill out the given variables
# below with target catalog/database if this is fine for you.
#
# ALSO NOTE: The example submits an unbounded background statement. Make sure to stop the
# statement in the Web UI afterward to clean up resources.

# Fill this with an environment you have write access to
TARGET_CATALOG = ""

# Fill this with a Kafka cluster you have write access to
TARGET_DATABASE = ""

# Fill this with names of the Kafka Topics you want to create
TARGET_TABLE1 = "PricePerProduct"
TARGET_TABLE2 = "PricePerCustomer"

# A table program example that demos how to pipe data into a table or multiple tables.
def run():
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)
  env = TableEnvironment.create(settings)

  env.use_catalog(TARGET_CATALOG)
  env.use_database(TARGET_DATABASE)

  print("Creating tables... %s" % [TARGET_TABLE1, TARGET_TABLE2])

  # Create two helper tables that will be filled with data from examples
  env.create_table(
      TARGET_TABLE1,
      ConfluentTableDescriptor.for_managed()
      .schema(
          Schema.new_builder()
          .column("product_id", DataTypes.STRING().not_null())
          .column("price", DataTypes.DOUBLE().not_null())
          .build())
      .distributed_into(1)
      .build())
  env.create_table(
      TARGET_TABLE2,
      ConfluentTableDescriptor.for_managed()
      .schema(
          Schema.new_builder()
          .column("customer_id", DataTypes.INT().not_null())
          .column("price", DataTypes.DOUBLE().not_null())
          .build())
      .distributed_into(1)
      .build())

  print("Executing table pipeline synchronous...")

  table_result = \
    env.from_elements(
        [row("1408", 27.71), row("1062", 94.39), row("42", 80.01)],
        DataTypes.ROW(
            [DataTypes.FIELD("customer_id", DataTypes.STRING()),
             DataTypes.FIELD("price", DataTypes.DOUBLE())])
    ) \
      .execute_insert(TARGET_TABLE1)

  # Execution happens async by default, use wait() to attach to the execution in case all
  # sources are finite (i.e. bounded).
  # For infinite (i.e. unbounded) sources, waiting for completion would not make much sense.
  table_result.wait()

  print("Executing statement set asynchronous...")

  # The API supports more than a single sink, you can also fan out to different tables while
  # reading from a table once using a StatementSet:
  statementSet = \
    env.create_statement_set() \
      .add_insert(
        TARGET_TABLE1,
        env.from_path("`examples`.`marketplace`.`orders`")
        .select(col("product_id"), col("price"))
    ) \
      .add_insert(
        TARGET_TABLE2,
        env.from_path("`examples`.`marketplace`.`orders`")
        .select(col("customer_id"), col("price"))
    )

  # Executes a statement set that splits the 'orders' table into two tables,
  # a 'product_id | price' table and a 'customer_id | price' one
  statementSet.execute()

  print("Reading merged data written by background statement...")

  # For this example, we read both target tables in again and union them into one output to
  # verify that the data arrives
  targetTable1 = \
    env.from_path(TARGET_TABLE1) \
      .select(concat(col("product_id"), lit(" event in "), lit(TARGET_TABLE1)))

  targetTable2 = \
    env.from_path(TARGET_TABLE2) \
      .select(
        concat(
            col("customer_id").cast(DataTypes.STRING()),
            lit(" event in "),
            lit(TARGET_TABLE2))
    )

  targetTable1.union_all(targetTable2).alias("status").execute().print()
