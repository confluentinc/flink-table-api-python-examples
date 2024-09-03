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

from pyflink.table import (TableEnvironment, Schema, DataTypes, FormatDescriptor)
from pyflink.table.confluent import ConfluentSettings, ConfluentTableDescriptor

CLOUD_PROPERTIES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     "../../../config/cloud.properties")

# Fill this with an environment you have write access to
TARGET_CATALOG = ""

# Fill this with a Kafka cluster you have write access to
TARGET_DATABASE = ""

# Fill this with names of the Kafka Topics you want to create
TARGET_TABLE1 = "MyExampleTable1"
TARGET_TABLE2 = "MyExampleTable2"

if __name__ == '__main__':
  settings = ConfluentSettings.from_file(CLOUD_PROPERTIES_PATH)

  t_env = TableEnvironment.create(settings)

  t_env.use_catalog(TARGET_CATALOG)
  t_env.use_database(TARGET_DATABASE)

  # Create a table programmatically:
  # The table...
  #   - is backed by an equally named Kafka topic
  #   - stores its payload in JSON
  #   - will reference two Schema Registry subjects for Kafka message key and value
  #   - is distributed across 4 Kafka partitions based on the Kafka message key "user_id"
  t_env.create_table(
      TARGET_TABLE1,
      ConfluentTableDescriptor.for_managed()
      .schema(
          Schema.new_builder()
          .column("user_id", DataTypes.STRING())
          .column("name", DataTypes.STRING())
          .column("email", DataTypes.STRING())
          .build())
      .distributed_by_into_buckets(4, "user_id")
      .key_format(FormatDescriptor.for_format("json-registry").build())
      .value_format(FormatDescriptor.for_format("json-registry").build())
      .build()
  )

  # Alternatively, the call above could also be executed with SQL
  t_env.executeSql(
      """CREATE TABLE IF NOT EXISTS 
      `%s`
      (
        `user_id` STRING,
        `name` STRING,
        `email` STRING
      ) DISTRIBUTED BY HASH(`user_id`) INTO 4 BUCKETS
      WITH (
        'kafka.retention.time' = '0 ms',
        'key.format' = 'json-registry',
        'value.format' = 'json-registry'
      )""" % TARGET_TABLE1)

# TODO: In python there is no ResolvedSchema and thus no way to retrieve only physical fields.
#   # The schema builders can be quite useful to avoid manual schema work. You can adopt schema
#   # from other tables, massage the schema, and/or add additional columns
#   productsRow = t_env.from_path("examples.marketplace.products") \
#   .get_schema() \
#   .toPhysicalRowDataType()
# List<String> columnNames = DataType.getFieldNames(productsRow);
# List<DataType> columnTypes = DataType.get_field_data_types(productsRow);
#
#   # In this example, the table will get all names/data types from the table 'products'
#   # plus an 'additionalColumn' column
#   t_env.createTable(
#     TARGET_TABLE2,
#     ConfluentTableDescriptor.for_managed()
#     .schema(
#         Schema.new_builder()
#         .from_fields(columnNames, columnTypes)
#         .column("additionalColumn", DataTypes.STRING())
#         .build())
#   .build())
