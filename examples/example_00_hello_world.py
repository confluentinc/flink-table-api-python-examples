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

from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table import TableEnvironment, Row
from pyflink.table.expressions import col, row


def run():
    # Setup connection properties to Confluent Cloud
    settings = ConfluentSettings.from_global_variables()
    env = TableEnvironment.create(settings)

    # Run your first Flink statement in Table API
    env.from_elements([row("Hello world!")]).execute().print()

    # Or use SQL
    env.sql_query("SELECT 'Hello world!'").execute().print()

    # Structure your code with Table objects - the main ingredient of Table API.
    table = (
        env.from_path("examples.marketplace.clicks")
        .filter(col("user_agent").like("Mozilla%"))
        .select(col("click_id"), col("user_id"))
    )

    table.print_schema()
    print(table.explain())

    # Use the provided tools to test on a subset of the streaming data
    expected = ConfluentTools.collect_materialized_limit(table, 50)
    actual = [Row(42, 500)]
    if expected != actual:
        print("Results don't match!")


if __name__ == "__main__":
    run()
