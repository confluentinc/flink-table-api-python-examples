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

from pyflink.table import (TableEnvironment, Schema, DataTypes, FormatDescriptor)
from pyflink.table.confluent import ConfluentSettings, ConfluentTableDescriptor
from flink_table_api_python.settings import CLOUD_PROPERTIES_PATH

# NOTE: This example requires write access to a Kafka cluster. Fill out the
# given variables below with target catalog/database if this is fine for you.

# Fill this with an environment you have write access to
TARGET_CATALOG = ""

# Fill this with a Kafka cluster you have write access to
TARGET_DATABASE = ""

# Fill this with names of the Kafka Topics you want to create
TARGET_TABLE1 = "MyExampleTable1"
TARGET_TABLE2 = "MyExampleTable2"

# A table program example that illustrates how to create a table backed
# by a Kafka topic.
def run():
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
  t_env.execute_sql(
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
      )""" % TARGET_TABLE2)
