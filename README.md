# Apache Flink® Table API on Confluent Cloud - Examples for Python

This repository contains examples for running Apache Flink's Table API on Confluent Cloud.

## Introduction to Table API

The [Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/) enables a programmatic
way of developing, testing, and submitting Flink pipelines for processing data streams.
Streams can be finite or infinite, with insert-only or changelog data. The latter allows for dealing with *Change Data
Capture* (CDC) events.

Within the API, you conceptually work with tables that change over time - inspired by relational databases. Write
a *Table Program* as a declarative and structured graph of data transformations. Table API is inspired by SQL and complements
it with additional tools for juggling real-time data. You can mix and match Flink SQL with Table API at any time as they
go hand in hand.

## Table API on Confluent Cloud

Table API on Confluent Cloud is a client-side library that delegates Flink API calls to Confluent’s public
REST API. It submits [Statements](https://docs.confluent.io/cloud/current/api.html#tag/Statements-(sqlv1)) and retrieves
[StatementResults](https://docs.confluent.io/cloud/current/api.html#tag/Statement-Results-(sqlv1)).

Table programs are implemented against [Flink's open source Table API for Python](https://github.com/apache/flink/tree/master/flink-python/pyflink/table).
The provided Confluent pip packages repackage the Python API and bundles the Confluent-specific components for powering the `TableEnvironment` without the need
for a local Flink cluster. By adding the `confluent-flink-table-api-java-plugin` dependency, Flink internal components such as
`CatalogStore`, `Catalog`, `Planner`, `Executor`, and configuration are managed by the plugin and fully integrate with
Confluent Cloud. Including access to Apache Kafka®, Schema Registry, and Flink Compute Pools.

Note: The Table API plugin is in Open Preview stage. Take a look at the [Known Limitation](#known-limitations) section below.

### Motivating Example

The following code shows how a Table API program is structured. Subsequent sections will go into more details how you
can use the examples of this repository to play around with Flink on Confluent Cloud.

```python
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table import TableEnvironment, Row
from pyflink.table.expressions import col, row

# A table program...
#   - uses Apache Flink's APIs
#   - communicates to Confluent Cloud via REST calls
def run():
  # Set up the connection to Confluent Cloud
  settings = ConfluentSettings.from_file("/cloud.properties")
  env = TableEnvironment.create(settings)

  # Run your first Flink statement in Table API
  env.from_elements([row("Hello world!")]).execute().print()

  # Or use SQL
  env.sql_query("SELECT 'Hello world!'").execute().print()

  # Structure your code with Table objects - the main ingredient of Table API.
  table = env.from_path("examples.marketplace.clicks") \
    .filter(col("user_agent").like("Mozilla%")) \
    .select(col("click_id"), col("user_id"))

  table.print_schema()
  print(table.explain())

  # Use the provided tools to test on a subset of the streaming data
  expected = ConfluentTools.collect_materialized_limit(table, 50)
  actual = [Row(42, 500)]
  if expected != actual:
      print("Results don't match!")
```

## Getting Started

### Prerequisites

1. Sign up for Confluent Cloud at [https://confluent.cloud](https://confluent.cloud/signup)
2. [Create a compute pool](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/create-compute-pool.html#create-a-compute-pool-in-ccloud-console)
   in the web UI of Confluent's Cloud Console
3. Optional: [Create a Kafka cluster](https://docs.confluent.io/cloud/current/clusters/create-cluster.html#manage-ak-clusters-on-ccloud)
   if you want to run examples that store data in Kafka

### Run Examples

All example files are located in `flink_table_api_python/examples/table`. Each file contains a `run()`
function with a table program that can be executed individually. Every example program covers a different topic to learn
more about how Table API can be used. It is recommended to go through the examples in the defined order as they partially
build on top of each other.

Clone this repository to your local computer, or download it as a ZIP file and extract it.
```bash
git clone https://github.com/confluentinc/flink-table-api-python-examples.git
```

Change the current directory.
```bash
cd flink-table-api-python-examples
```

Use [poetry](https://python-poetry.org/) to build a virtual environment containing all required dependencies and project files.

```bash
poetry install
```

Run an example script. No worries the program is read-only so it won't affect your existing
Kafka clusters. All results will be printed to the console.
```bash
poetry run example_00_hello_world
```

An output similar to the following means that you are able to run the examples:
```text
io.confluent.flink.plugin.ConfluentFlinkException: Parameter 'client.organization-id' not found.
```
Configuration will be covered in the next section.

### Configure the `cloud.properties` File

The Table API plugin needs a set of configuration options for establishing a connection to Confluent Cloud.

For experimenting with Table API, configuration with a properties file might be the most convenient option.
The examples read from this file by default.

Update the file under `config/cloud.properties` with your Confluent Cloud information.

All required information can be found in the web UI of Confluent's Cloud Console:
- `client.organization-id` from [**Menu** → **Settings** → **Organizations**](https://confluent.cloud/settings/organizations)
- `client.environment-id` from [**Menu** → **Environments**](https://confluent.cloud/environments)
- `client.cloud`, `client.region`, `client.compute-pool-id` from [**Menu** → **Environments**](https://confluent.cloud/environments) → **your environment** → **Flink** → **your compute pool**
- `client.flink-api-key`, `client.flink-api-secret` from [**Menu** → **Settings** → **API keys**](https://confluent.cloud/settings/api-keys)

Examples should be runnable after setting all configuration options correctly.

## Configuration

The Table API plugin needs a set of configuration options for establishing a connection to Confluent Cloud.

The `ConfluentSettings` class is a utility for providing configuration options from various sources.

For production, external input, code, and environment variables can be combined.

Precedence order (highest to lowest):
1. Properties File
2. Code
3. Environment Variables

A multi-layered configuration can look like:
```python
from pyflink.table.confluent import ConfluentSettings
from pyflink.table import TableEnvironment

def run():
  # Args might set cloud, region, org, env, and compute pool.
  # Environment variables might pass key and secret.

  # Code sets the session name and SQL-specific options.
  settings = ConfluentSettings.new_builder_from_file(...) \
    .set_context_name("MyTableProgram") \
    .set_option("sql.local-time-zone", "UTC") \
    .build()

  env = TableEnvironment.create(settings)
```

### Via Properties File

Store options (or some options) in a `cloud.properties` file:

```properties
# Cloud region
client.cloud=aws
client.region=eu-west-1

# Access & compute resources
client.flink-api-key=XXXXXXXXXXXXXXXX
client.flink-api-secret=XxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXx
client.organization-id=00000000-0000-0000-0000-000000000000
client.environment-id=env-xxxxx
client.compute-pool-id=lfcp-xxxxxxxxxx
```

Reference the `cloud.properties` file:
```python
from pyflink.table.confluent import ConfluentSettings

# Arbitrary file location in file system
settings = ConfluentSettings.from_file("/path/to/cloud.properties")
```

### Via Code

Pass all options (or some options) in code:

```python
from pyflink.table.confluent import ConfluentSettings

settings = ConfluentSettings.new_builder() \
  .set_cloud("aws") \
  .set_region("us-east-1") \
  .set_flink_api_key("key") \
  .set_flink_api_secret("secret") \
  .set_organization_id("b0b21724-4586-4a07-b787-d0bb5aacbf87") \
  .set_environment_id("env-z3y2x1") \
  .set_compute_pool_id("lfcp-8m03rm") \
  .build()
```

### Via Environment Variables

Pass all options (or some options) as variables:

```bash
export CLOUD_PROVIDER="aws"
export CLOUD_REGION="us-east-1"
export FLINK_API_KEY="key"
export FLINK_API_SECRET="secret"
export ORG_ID="b0b21724-4586-4a07-b787-d0bb5aacbf87"
export ENV_ID="env-z3y2x1"
export COMPUTE_POOL_ID="lfcp-8m03rm"

poetry run example
```

In code call:
```python
from pyflink.table.confluent import ConfluentSettings

settings = ConfluentSettings.from_global_variables()
```

### Configuration Options

The following configuration needs to be provided:

| Property key              | Environment variable | Required | Comment                                                                      |
|---------------------------|----------------------|----------|------------------------------------------------------------------------------|
| `client.cloud`            | `CLOUD_PROVIDER`     | Y        | Confluent identifier for a cloud provider. For example: `aws`                |
| `client.region`           | `CLOUD_REGION`       | Y        | Confluent identifier for a cloud provider's region. For example: `us-east-1` |
| `client.flink-api-key`    | `FLINK_API_KEY`      | Y        | API key for Flink access.                                                    |
| `client.flink-api-secret` | `FLINK_API_SECRET`   | Y        | API secret for Flink access.                                                 |
| `client.organization`     | `ORG_ID`             | Y        | ID of the organization. For example: `b0b21724-4586-4a07-b787-d0bb5aacbf87`  |
| `client.environment`      | `ENV_ID`             | Y        | ID of the environment. For example: `env-z3y2x1`                             |
| `client.compute-pool`     | `COMPUTE_POOL_ID`    | Y        | ID of the compute pool. For example: `lfcp-8m03rm`                           |

Additional configuration:

| Property key            | Environment variable | Required | Comment                                                                                                  |
|-------------------------|----------------------|----------|----------------------------------------------------------------------------------------------------------|
| `client.principal`      | `PRINCIPAL_ID`       | N        | Principal that runs submitted statements. For example: `sa-23kgz4` (for a service account)               |
| `client.context`        |                      | N        | A name for this Table API session. For example: `my_table_program`                                       |
| `client.statement-name` |                      | N        | Unique name for statement submission. By default, generated using a UUID.                                |
| `client.rest-endpoint`  | `REST_ENDPOINT`      | N        | URL to the REST endpoint. For example: `proxyto.confluent.cloud`                                         |
| `client.catalog-cache`  |                      | N        | Expiration time for catalog objects. For example: '5 min'. '1 min' by default. '0' disables the caching. |

## Documentation for Confluent Utilities

### Confluent Tools

The `ConfluentTools` class adds additional methods that can be useful when developing and testing Table API programs.

#### `ConfluentTools.collect_changelog` / `ConfluentTools.print_changelog`

Executes the given table transformations on Confluent Cloud and returns the results locally
as a list of changelog rows. Or prints to the console in a table style.

This method performs `table.execute().collect()` under the hood and consumes a fixed
amount of rows from the returned iterator.

Note: The method can work on both finite and infinite input tables. If the pipeline is
potentially unbounded, it will stop fetching after the desired amount of rows has been
reached.

Examples:
```python
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table import TableEnvironment

settings = ConfluentSettings.from_global_variables()
env = TableEnvironment.create(settings)
# On Table object
table = env.from_path("examples.marketplace.customers")
rows = ConfluentTools.collect_changelog_limit(table, 100)
ConfluentTools.print_changelog_limit(table, 100)

# On TableResult object
tableResult = env.execute_sql("SELECT * FROM examples.marketplace.customers")
rows = ConfluentTools.collect_changelog_limit(tableResult, 100)
ConfluentTools.print_changelog_limit(tableResult, 100)
```

Shortcuts:
```python
# For finite (i.e. bounded) tables
ConfluentTools.collect_changelog(table)
ConfluentTools.print_changelog(table)
```

#### `ConfluentTools.collect_materialized` / `ConfluentTools.print_materialized`

Executes the given table transformations on Confluent Cloud and returns the results locally
as a materialized changelog. In other words: changes are applied to an in-memory table and
returned as a list of insert-only rows. Or printed to the console in a table style.

This method performs `table.execute().collect()` under the hood and consumes a fixed
amount of rows from the returned iterator.

Note: The method can work on both finite and infinite input tables. If the pipeline is
potentially unbounded, it will stop fetching after the desired amount of rows has been
reached.

```python
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table import TableEnvironment

settings = ConfluentSettings.from_global_variables()
env = TableEnvironment.create(settings)
# On Table object
table = env.from_path("examples.marketplace.customers")
rows = ConfluentTools.collect_materialized_limit(table, 100)
ConfluentTools.print_materialized_limit(table, 100)

# On TableResult object
tableResult = env.execute_sql("SELECT * FROM examples.marketplace.customers")
rows = ConfluentTools.collect_materialized_limit(tableResult, 100)
ConfluentTools.print_materialized_limit(tableResult, 100)
```

Shortcuts:
```python
# For finite (i.e. bounded) tables
ConfluentTools.collect_materialized(table)
ConfluentTools.print_materialized(table)
```

### Confluent Table Descriptor

A table descriptor for creating tables located in Confluent Cloud programmatically.

Compared to the regular Flink one, this class adds support for Confluent's system columns
and convenience methods for working with Confluent tables.

`for_managed` corresponds to `TableDescriptor.for_conector("confluent")`.

```python
from pyflink.table.confluent import ConfluentTableDescriptor
from pyflink.table import Schema, DataTypes
from pyflink.table.expressions import col, lit

descriptor = ConfluentTableDescriptor.for_managed() \
  .schema(
    Schema.new_builder()
      .column("i", DataTypes.INT())
      .column("s", DataTypes.INT())
      .watermark("$rowtime", col("$rowtime").minus(lit(5).seconds)) # Access $rowtime system column
      .build()) \
  .build()

env.createTable("t1", descriptor)
```

## Known Limitations

The Table API plugin is in Open Preview stage.

### Unsupported by Table API Plugin

The following features are currently not supported:

- Temporary catalog objects (including tables, views, functions)
- Custom modules
- Custom catalogs
- User-defined functions (including system functions)
- Anonymous, inline objects (including functions, data types)
- CompiledPlan features are not supported
- Batch mode
- Restrictions coming from Confluent Cloud
    - custom connectors/formats, including: 
      - from_elements with Python objects
      - converting to/from_pandas
    - processing time operations
    - structured data types
    - many configuration options
    - limited SQL syntax
    - batch execution mode

### Issues in Open Source Flink

- Both catalog/database must be set or identifiers must be fully qualified. A mixture of setting a current catalog and
  using two-part identifiers can lead to errors.
- String concatenation with `.plus` leads to errors. Use `Expressions.concat`.
- Selecting `.rowtime` in windows leads to errors.
- Using `.limit()` can lead to errors.
- Python API is not fully on par with Java API. The API lacks support for: TablePipeline, ResolvedSchema

### Supported API

The following API methods are considered stable and ready to be used:

```text
// TableEnvironment
TableEnvironment.create_statement_st()
TableEnvironment.create_table(String, TableDescriptor)
TableEnvironment.execute_sql(String)
TableEnvironment.explain_sql(String)
TableEnvironment.from_path(String)
TableEnvironment.get_config()
TableEnvironment.get_current_catalog()
TableEnvironment.get_current_database()
TableEnvironment.list_catalogs()
TableEnvironment.list_databases()
TableEnvironment.list_functions()
TableEnvironment.list_tables()
TableEnvironment.list_views()
TableEnvironment.sql_query(String)
TableEnvironment.use_catalog(String)
TableEnvironment.use_database(String)

// from_elements works partially, it should be safe to use it in combination with
// pyflink.table.expression, passing Python objects is not supported
TableEnvironment.from_elements(...)

// Table: SQL equivalents
Table.select(...)
Table.alias(...)
Table.filter(...)
Table.where(...)
Table.group_by(...)
Table.distinct()
Table.join(...)
Table.left_outer_join(...)
Table.right_outer_join(...)
Table.full_outer_join(...)
Table.minus(...)
Table.minus_all(...)
Table.union(...)
Table.union_all(...)
Table.intersect(...)
Table.intersect_all(...)
Table.order_by(...)
Table.offset(...)
Table.fetch(...)
Table.limit(...)
Table.window(...)

// Table: API extensions
Table.print_schema()
Table.add_columns(...)
Table.add_or_replace_columns(...)
Table.rename_columns(...)
Table.drop_columns(...)
Table.explain()
Table.execute()
Table.execute_insert(...)

// StatementSet
StatementSet.execute()
StatementSet.add_insert(...)
StatementSet.add_insert_sql(...)

// TableResult
TableResult.get_job_client().cancel()
TableResult.wait(...)
TableResult.collect()
TableResult.print()

// TableConfig
TableConfig.set(...)

// Expressions
Expressions.* (except for call())

// Others
TableDescriptor.*
FormatDescriptor.*
Tumble.*
Slide.*
Session.*
Over.*
```

Confluent adds the following classes for more convenience:
```text
ConfluentSettings.*
ConfluentTools.*
ConfluentTableDescriptor.*
```

## Support

Table API goes hand in hand with Flink SQL on Confluent Cloud.
For feature requests or support tickets, use one of the [established channels](https://docs.confluent.io/cloud/current/flink/get-help.html). 