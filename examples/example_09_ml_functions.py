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

from confluent_pyflink.table import TableEnvironment, DataTypes
from confluent_pyflink.table.utils import ConfluentSettings
from confluent_pyflink.table.expressions import col, lit, array, row

from confluent_pyflink.table.confluent_ml_expressions import (
    ml_label_encoder,
    ml_one_hot_encoder,
    ml_min_max_scaler,
    ml_standard_scaler,
    ml_bucketize,
    ai_embedding,
)

# The model-backed example here uses an AWS bedrock model. Set the AWS credentials to run
# it, or replace it with a different provider. Connection and model creation also require
# an environment and database to be set.
TARGET_CATALOG = ""
TARGET_DATABASE = ""

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
AWS_SESSION_TOKEN = ""
BEDROCK_ENDPOINT = "https://bedrock-runtime.<REGION>.amazonaws.com/model/<MODEL_ID>/invoke"

CONNECTION_NAME = "bedrock_connection"
MODEL_NAME = "bedrock_embed"


# The built-in transformation functions are pure Flink functions.
def built_in_transformations(env):
    products = env.from_elements(
        [
            row("Espresso Machine", "kitchen", 249.0),
            row("Yoga Mat", "fitness", 19.99),
            row("Standing Desk", "office", 410.5),
            row("Water Bottle", "fitness", 12.0),
            row("Monitor", "office", 189.0),
        ],
        DataTypes.ROW(
            [
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("category", DataTypes.STRING()),
                DataTypes.FIELD("price", DataTypes.DOUBLE()),
            ]
        ),
    )

    # `ml_label_encoder` encodes categorical variables into numerical labels.
    # `ml_one_hot_encoder` encodes categorical variables into a binary vector representation.
    categories = array("kitchen", "fitness", "office")
    products.select(
        col("name"),
        ml_label_encoder(col("category"), categories).alias("category_label"),
        ml_one_hot_encoder(col("category"), categories).alias("category_onehot"),
    ).execute().print()

    # Scalers scale numerical values, either to a specified range using min-max normalization
    # in the case of `ml_min_max_scaler`, or by removing the mean and scaling to unit variance
    # in the case of `ml_standard_scaler`.
    products.select(
        col("name"),
        ml_min_max_scaler(col("price"), lit(12.0), lit(410.5)).alias("price_minmax"),
        ml_standard_scaler(col("price"), lit(176.1), lit(147.0)).alias("price_standard"),
    ).execute().print()

    # Bucketise the input column into 3 buckets: [-inf, 50), [50, 200), [200, inf).
    products.select(
        col("name"),
        col("price"),
        ml_bucketize(col("price"), array(50.0, 200.0)).alias("price_bucket"),
    ).execute().print()


# The model-backed functions invoke a remote model that must first be registered with
# CREATE MODEL.
def model_backed_functions(env):
    env.use_catalog(TARGET_CATALOG)
    env.use_database(TARGET_DATABASE)

    env.execute_sql(
        f"""
        CREATE CONNECTION IF NOT EXISTS `{CONNECTION_NAME}`
        WITH (
            'type' = 'bedrock',
            'endpoint' = '{BEDROCK_ENDPOINT}',
            'aws-access-key' = '{AWS_ACCESS_KEY}',
            'aws-secret-key' = '{AWS_SECRET_KEY}',
            'aws-session-token' = '{AWS_SESSION_TOKEN}'
        )
        """
    )

    env.execute_sql(
        f"""
        CREATE MODEL IF NOT EXISTS `{MODEL_NAME}`
        INPUT (text STRING)
        OUTPUT (embedding ARRAY<FLOAT>)
        WITH (
            'provider' = 'bedrock',
            'task' = 'embedding',
            'bedrock.connection' = '{CONNECTION_NAME}',
            'bedrock.input_format' = 'AMAZON-TITAN-EMBED'
        )
        """
    )

    documents = env.from_elements(
        [
            row("Apache Flink is a distributed processing engine for data streams."),
            row("Logs and tables are sort of the same thing, if you think about it."),
            row("Monads are like burritos."),
        ],
        DataTypes.ROW([DataTypes.FIELD("text", DataTypes.STRING())]),
    )

    # ai_embedding() is a table function, so it is invoked via join_lateral(). Scalar functions
    # such as ai_sentiment() would instead be used directly in select().
    documents.join_lateral(ai_embedding(MODEL_NAME, col("text")).alias("embedding")).select(
        col("text"), col("embedding")
    ).execute().print()


def run():
    settings = ConfluentSettings()
    env = TableEnvironment.create(settings)

    built_in_transformations(env)

    if AWS_ACCESS_KEY:
        model_backed_functions(env)


if __name__ == "__main__":
    run()
