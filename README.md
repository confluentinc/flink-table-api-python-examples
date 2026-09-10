# Apache Flink® Table API on Confluent Cloud - Examples

This repository contains examples for running Apache Flink's Table API on Confluent Cloud.

## Introduction to Table API for Python

The [Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table_api_tutorial/) enables a programmatic
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
The provided Confluent pip packages repackage Flink's Python API and bundle the Confluent-specific components for powering the `TableEnvironment` without the need
for a local Flink cluster. While using those packages, Flink internal components such as
`CatalogStore`, `Catalog`, `Planner`, `Executor`, and configuration are managed by the plugin and fully integrate with
Confluent Cloud. Including access to Apache Kafka®, Schema Registry, and Flink Compute Pools.

Note: The Table API plugin is in Open Preview stage. Take a look at the [Known Limitation](#known-limitations) section below.

### Motivating Example

The following code shows how a Table API program is structured. Subsequent sections will go into more details how you
can use the examples of this repository to play around with Flink on Confluent Cloud.

```python
from confluent_pyflink.table.utils import ConfluentSettings, ConfluentTools
from confluent_pyflink.table import TableEnvironment, Row
from confluent_pyflink.table.expressions import col, row

def run():
    # Setup connection properties to Confluent Cloud
    settings = ConfluentSettings()
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

if __name__ == "__main__":
    run()
```

## Getting Started

### Prerequisites

1. Sign up for Confluent Cloud at [https://confluent.cloud](https://confluent.cloud/signup)
2. [Create a compute pool](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/create-compute-pool.html#create-a-compute-pool-in-ccloud-console)
   in the web UI of Confluent's Cloud Console
3. [Generate an API Key](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/generate-api-key-for-flink.html#generate-an-api-key)
   for the region where you created your compute pool
4. Optional: [Create a Kafka cluster](https://docs.confluent.io/cloud/current/clusters/create-cluster.html#manage-ak-clusters-on-ccloud)
   if you want to run examples that store data in Kafka
5. Have the correct environment variables set as per [the documentation](https://docs.confluent.io/cloud/current/flink/reference/table-api.html#environment-variables)
6. We recommend using a tool like [uv](https://docs.astral.sh/uv/) to manage your Python versions and environments and Python 3.9-3.11 are the only versions currently supported.

### Run Examples

All example files are located in `examples`. Each file contains a `run()`
function that can be executed directly or in `__main__`. Each has multiple table programs that will be executed individually. Every example program covers a different topic to learn
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

We recommend using [uv](https://docs.astral.sh/uv/) to run the scripts, will automatically create a virtualenv with the required dependencies.

**Note**: Flink's Python API communicates with a Java process under the hood. Make sure you also have at least Java 11
installed. Check that your `JAVA_HOME` environment variable is correctly set. Only checking `java -version` might not
be enough.

```
echo $JAVA_HOME
```

If required install openjdk and export the JAVA_HOME
```bash
brew install openjdk && export JAVA_HOME=$(/usr/libexec/java_home) && echo $JAVA_HOME
```

Run an example script. No worries the program is read-only so it won't affect your existing
Kafka clusters. All results will be printed to the console.
```bash
uv run examples/example_00_hello_world
```

An output similar to the following means that you are able to run the examples:
```text
ConfluentSettingsValidationError: 5 validation errors for ConfluentSettings
Missing the following required configuration keys: org_id, env_id, compute_pool_id, cloud_provider, cloud_region
```
Configuration will be covered in the next section.

### Configure the settings parameters in the `ConfluentSettings` class.

`ConfluentSettings` needs a set of configuration options for establishing a connection to Confluent Cloud. These can be set via environment variables, a `.env` file, a JSON/YAML file, or as keyword arguments in code. This example uses the environment variables. For more details, please see the [documentation](https://docs.confluent.io/cloud/current/flink/reference/table-api.html#confluentsettings-class).

All required information can be found in the web UI of Confluent's Cloud Console:
- `CONFLUENT_ORG_ID` from [**Menu** → **Settings** → **Organizations**](https://confluent.cloud/settings/organizations)
- `CONFLUENT_ENV_ID` from [**Menu** → **Environments**](https://confluent.cloud/environments)
- `CONFLUENT_CLOUD_PROVIDER`, `CONFLUENT_CLOUD_REGION`, `CONFLUENT_COMPUTE_POOL_ID` from [**Menu** → **Environments**](https://confluent.cloud/environments) → **your environment** → **Flink** → **your compute pool**
- `CONFLUENT_FLINK_API_KEY`, `CONFLUENT_FLINK_API_SECRET` from [**Menu** → **Settings** → **API keys**](https://confluent.cloud/settings/api-keys)

Export the environment variables as shown below:

```bash
export CONFLUENT_CLOUD_PROVIDER="<my_cloud>"
export CONFLUENT_CLOUD_REGION="<my_region>"
export CONFLUENT_FLINK_API_KEY="<my_key>"
export CONFLUENT_FLINK_API_SECRET="<my_secret>"
export CONFLUENT_ORG_ID="<my_organization>"
export CONFLUENT_ENV_ID="<my_environment>"
export CONFLUENT_COMPUTE_POOL_ID="<my_compute_pool>"
```

Alternatively, copy `.env.example` to `.env` and fill in the same values. Examples should be runnable after setting all configuration options correctly.

### Table API Playground using Python Interactive Shell

For convenience, the repository also contains an init script for playing around with
Table API in an interactive manner.

1. Create a virtualenv with `uv sync` and activate it with `source .venv/bin/activate`.

2. Run `python -i start_pyshell.py` to start an interactive repl to explore Table API.

3. The `TableEnvironment` is pre-initialized from environment variables and available under `env`.

4. Run your first "Hello world!" using `env.execute_sql("SELECT 'Hello world!'").print()`

## Configuration

The Table API plugin needs a set of configuration options for establishing a connection to Confluent Cloud.

`ConfluentSettings` is a Pydantic settings model. Values are resolved from keyword arguments, environment variables (with a `CONFLUENT_` prefix), a `.env` file, or a JSON/YAML file, and these sources can be combined.

Precedence order (highest to lowest):
1. Keyword arguments passed in code — this includes everything loaded from a file via `from_file`, since the file's contents and any overrides are applied as constructor arguments. A value set in a JSON/YAML file therefore takes precedence over an environment variable of the same name.
2. Environment variables (`CONFLUENT_*`)
3. `.env` file in the working directory

A multi-layered configuration can look like:
```python
from confluent_pyflink.table.utils import ConfluentSettings
from confluent_pyflink.table import TableEnvironment

def run():
  # A JSON/YAML file might set cloud, region, org, env, and compute pool.
  # Environment variables (CONFLUENT_*) can supply the remaining values, such
  # as the API key and secret.

  # Keyword overrides take precedence over both the file and the environment.
  settings = ConfluentSettings.from_file(
    "/path/to/cloud.json",
    application_name="my-table-program",
  )

  env = TableEnvironment.create(settings)
```

### Via a Configuration File

Store options (or some options) in a JSON or YAML file, keyed by setting name. For example `cloud.json`:

```json
{
  "cloud_provider": "aws",
  "cloud_region": "us-east-1",
  "flink_api_key": "key",
  "flink_api_secret": "secret",
  "org_id": "b0b21724-4586-4a07-b787-d0bb5aacbf87",
  "env_id": "env-z3y2x1",
  "compute_pool_id": "lfcp-8m03rm"
}
```

Reference the file:
```python
from confluent_pyflink.table.utils import ConfluentSettings

# Arbitrary file location in the file system
settings = ConfluentSettings.from_file("/path/to/cloud.json")
```

### Via Code

Pass all options (or some options) as keyword arguments in code:

```python
from confluent_pyflink.table.utils import ConfluentSettings

settings = ConfluentSettings(
  cloud_provider="aws",
  cloud_region="us-east-1",
  flink_api_key="key",
  flink_api_secret="secret",
  org_id="b0b21724-4586-4a07-b787-d0bb5aacbf87",
  env_id="env-z3y2x1",
  compute_pool_id="lfcp-8m03rm",
)
```

### Via Environment Variables

Pass all options (or some options) as `CONFLUENT_`-prefixed variables:

```bash
export CONFLUENT_CLOUD_PROVIDER="aws"
export CONFLUENT_CLOUD_REGION="us-east-1"
export CONFLUENT_FLINK_API_KEY="key"
export CONFLUENT_FLINK_API_SECRET="secret"
export CONFLUENT_ORG_ID="b0b21724-4586-4a07-b787-d0bb5aacbf87"
export CONFLUENT_ENV_ID="env-z3y2x1"
export CONFLUENT_COMPUTE_POOL_ID="lfcp-8m03rm"
```

The same variables can instead be placed in a `.env` file in the working directory. In code call:
```python
from confluent_pyflink.table.utils import ConfluentSettings

# Reads the CONFLUENT_* environment variables (and a .env file if present)
settings = ConfluentSettings()
```

### Configuration Options

Every setting can be provided as a keyword argument (`snake_case`), an environment variable
(`CONFLUENT_` + upper snake case), or a key in a JSON/YAML file.

#### Required connection settings

| Setting           | Environment variable        | Required | Comment                                                                     |
|-------------------|-----------------------------|----------|-----------------------------------------------------------------------------|
| `cloud_provider`  | `CONFLUENT_CLOUD_PROVIDER`  | Y        | Confluent identifier for a cloud provider. One of: `aws`, `gcp`, `azure`    |
| `cloud_region`    | `CONFLUENT_CLOUD_REGION`    | Y        | Cloud provider's region. For example: `us-east-1`                           |
| `org_id`          | `CONFLUENT_ORG_ID`          | Y        | ID of the organization. For example: `b0b21724-4586-4a07-b787-d0bb5aacbf87` |
| `env_id`          | `CONFLUENT_ENV_ID`          | Y        | ID of the environment. For example: `env-z3y2x1`                            |
| `compute_pool_id` | `CONFLUENT_COMPUTE_POOL_ID` | Y        | ID of the compute pool. For example: `lfcp-8m03rm`                          |

#### Authentication

`auth_mode` selects how the client authenticates; the default is `api-key`.

| Setting     | Environment variable  | Required | Comment                                                                    |
|-------------|-----------------------|----------|----------------------------------------------------------------------------|
| `auth_mode` | `CONFLUENT_AUTH_MODE` | N        | `api-key` (default), `oauth-client-credentials`, or `oauth-static-token`.  |

For `api-key` auth (the default):

| Setting               | Environment variable            | Required | Comment                                                                       |
|-----------------------|---------------------------------|----------|-------------------------------------------------------------------------------|
| `flink_api_key`       | `CONFLUENT_FLINK_API_KEY`       | Y¹       | API key for Flink access.                                                     |
| `flink_api_secret`    | `CONFLUENT_FLINK_API_SECRET`    | Y¹       | API secret for Flink access.                                                  |
| `global_api_key`      | `CONFLUENT_GLOBAL_API_KEY`      | N        | Key for both Flink and Artifact access; takes precedence over dedicated keys. |
| `global_api_secret`   | `CONFLUENT_GLOBAL_API_SECRET`   | N        | Secret for both Flink and Artifact access.                                    |
| `artifact_api_key`    | `CONFLUENT_ARTIFACT_API_KEY`    | N        | Key for Artifact creation (UDF uploads), when no global key is set.           |
| `artifact_api_secret` | `CONFLUENT_ARTIFACT_API_SECRET` | N        | Secret for Artifact creation (UDF uploads), when no global secret is set.     |

¹ Required for `api-key` auth unless a `global_api_key`/`global_api_secret` pair is provided.

For OAuth auth (`oauth-client-credentials` or `oauth-static-token`):

| Setting                        | Environment variable                     | Required | Comment                                                                    |
|--------------------------------|------------------------------------------|----------|----------------------------------------------------------------------------|
| `oauth_identity_pool_id`       | `CONFLUENT_OAUTH_IDENTITY_POOL_ID`       | Y²       | Confluent Cloud identity pool ID. For example: `pool-xxxxx`.               |
| `oauth_external_token_url`     | `CONFLUENT_OAUTH_EXTERNAL_TOKEN_URL`     | Y³       | External IdP OAuth 2.0 token endpoint URL.                                 |
| `oauth_external_client_id`     | `CONFLUENT_OAUTH_EXTERNAL_CLIENT_ID`     | Y³       | Client ID registered with the external IdP.                               |
| `oauth_external_client_secret` | `CONFLUENT_OAUTH_EXTERNAL_CLIENT_SECRET` | Y³       | Client secret registered with the external IdP.                           |
| `oauth_external_token_scope`   | `CONFLUENT_OAUTH_EXTERNAL_TOKEN_SCOPE`   | N        | OAuth scope to request from the IdP (IdP-dependent).                       |
| `oauth_external_access_token`  | `CONFLUENT_OAUTH_EXTERNAL_ACCESS_TOKEN`  | Y⁴       | Pre-issued OAuth bearer token (not refreshed by the client).              |

² Required for any OAuth mode.
³ Required for `oauth-client-credentials`.
⁴ Required for `oauth-static-token`.

#### Additional settings

| Setting                      | Environment variable                   | Required | Comment                                                                                                   |
|------------------------------|----------------------------------------|----------|-----------------------------------------------------------------------------------------------------------|
| `application_name`           | `CONFLUENT_APPLICATION_NAME`           | N        | Namespace/prefix for statement names submitted by this application.                                       |
| `statement_name`             | `CONFLUENT_STATEMENT_NAME`             | N        | Name for the next statement submission. By default, generated using a UUID.                               |
| `principal_id`               | `CONFLUENT_PRINCIPAL_ID`               | N        | Principal that runs submitted statements. For example: `sa-23kgz4` (service account).                     |
| `on_conflict`                | `CONFLUENT_ON_CONFLICT`                | N        | `fail` (default) or `replace`. `replace` requires `application_name`.                                     |
| `catalog_cache`              | `CONFLUENT_CATALOG_CACHE`              | N        | Expiration for catalog objects. Default `1 min`; `0` disables caching. See the duration note below.       |
| `timeout`                    | `CONFLUENT_TIMEOUT`                    | N        | Max wait for statement lifecycle actions. Default `15 min`. See the duration note below.                  |
| `endpoint_template`          | `CONFLUENT_ENDPOINT_TEMPLATE`          | N        | Template for the endpoint URL. Default `https://flink.{region}.{cloud}.confluent.cloud`.                  |
| `artifact_endpoint_template` | `CONFLUENT_ARTIFACT_ENDPOINT_TEMPLATE` | N        | Template for the artifact endpoint URL. Default `https://api.confluent.cloud`.                            |
| `options`                    | `CONFLUENT_OPTIONS`                    | N        | Extra Confluent options not exposed as dedicated fields (a dict; does not support Flink-native options).  |
| `http_user_agent`            | `CONFLUENT_HTTP_USER_AGENT`            | N        | Custom HTTP User-Agent header for API requests (advanced).                                                |

> **Duration format:** `catalog_cache` and `timeout` are `timedelta` values. As a keyword argument pass a `datetime.timedelta`.
  As an environment variable or file value use an ISO-8601 duration (e.g. `PT5M`, `PT15M`) or `HH:MM:SS` (e.g. `0:05:00`).

### Endpoint Configuration

`ConfluentSettings` provides options to configure endpoints for connecting to Confluent Cloud services.

### `endpoint_template`

This option provides a template for constructing the Flink statement API endpoint URL.

- **Default**: `https://flink.{region}.{cloud}.confluent.cloud`
- **Example**: `https://flinkpls-dom123.{region}.{cloud}.confluent.cloud`
- **Usage**: The template supports placeholders `{region}` and `{cloud}` that are replaced with the configured region and cloud provider values.
- **Environment Variable**: `CONFLUENT_ENDPOINT_TEMPLATE`

### `artifact_endpoint_template`

Template for the artifact (UDF upload) endpoint URL, using the same `{region}`/`{cloud}` placeholders.

- **Default**: `https://api.confluent.cloud`
- **Environment Variable**: `CONFLUENT_ARTIFACT_ENDPOINT_TEMPLATE`

Both endpoints fall back to their defaults when not set: `https://flink.{region}.{cloud}.confluent.cloud`
for the statement API and `https://api.confluent.cloud` for artifacts.

### Example

Here's a simple example showing how to configure an endpoint:

```python
# cloud.json:
# {
#   "cloud_region": "us-east-1",
#   "cloud_provider": "aws",
#   "endpoint_template": "https://flinkpls-dom123.{region}.{cloud}.confluent.cloud"
# }

# Resolved endpoints:
# - Statement API: https://flinkpls-dom123.us-east-1.aws.confluent.cloud
settings = ConfluentSettings.from_file("/cloud.json")
```

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
from confluent_pyflink.table.utils import ConfluentSettings, ConfluentTools
from confluent_pyflink.table import TableEnvironment

settings = ConfluentSettings()
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
from confluent_pyflink.table.utils import ConfluentSettings, ConfluentTools
from confluent_pyflink.table import TableEnvironment

settings = ConfluentSettings()
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

### `ConfluentTools.get_statement_name` / `ConfluentTools.stop_statement`

Additional lifecycle methods are available to control statements on Confluent Cloud after they have
been submitted.

```python
# On TableResult object
table_result = env.execute_sql("SELECT * FROM examples.marketplace.customers")
statement_name = ConfluentTools.get_statement_name(table_result)
ConfluentTools.stop_statement(table_result)

# Based on statement name
handle = ConfluentTools.get_statement_handle_by_name(env, "table-api-2024-03-21-150457-36e0dbb2e366-sql")
handle.stop()
```

### Confluent Table Descriptor

A table descriptor for creating tables located in Confluent Cloud programmatically.

Compared to the regular Flink one, this class adds support for Confluent's system columns
and convenience methods for working with Confluent tables.

`for_managed` corresponds to `TableDescriptor.for_connector("confluent")`.

```python
from confluent_pyflink.table import Schema, DataTypes, TableDescriptor
from confluent_pyflink.table.expressions import col, lit

descriptor = TableDescriptor.for_managed() \
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
- Python API is not fully on par with the Java API. The API lacks support for: TablePipeline, ResolvedSchema

### Supported API

The following API methods are considered stable and ready to be used:

```text
// TableEnvironment  (optional args shown as name=...)
TableEnvironment.create_statement_set()
TableEnvironment.create_table(path, descriptor)
TableEnvironment.execute_sql(stmt)
TableEnvironment.explain_sql(stmt)
TableEnvironment.from_path(path)
TableEnvironment.get_config()
TableEnvironment.get_current_catalog()
TableEnvironment.get_current_database()
TableEnvironment.list_catalogs()
TableEnvironment.list_databases()
TableEnvironment.list_functions()
TableEnvironment.list_tables()
TableEnvironment.list_views()
TableEnvironment.sql_query(query)
TableEnvironment.use_catalog(catalog_name)
TableEnvironment.use_database(database_name)

// from_elements works partially, it should be safe to use it in combination with
// confluent_pyflink.table.expressions, passing Python objects is not supported
TableEnvironment.from_elements(elements, schema=...)

// Table: SQL equivalents
Table.select(*fields)
Table.alias(field, *fields)
Table.filter(predicate)
Table.where(predicate)
Table.group_by(*fields)
Table.distinct()
Table.join(right, join_predicate=...)
Table.left_outer_join(right, join_predicate=...)
Table.right_outer_join(right, join_predicate)
Table.full_outer_join(right, join_predicate)
Table.minus(right)
Table.minus_all(right)
Table.union(right)
Table.union_all(right)
Table.intersect(right)
Table.intersect_all(right)
Table.order_by(*fields)
Table.offset(offset)
Table.fetch(fetch)
Table.limit(fetch, offset=...)
Table.window(group_window)

// Table: API extensions
Table.print_schema()
Table.add_columns(*fields)
Table.add_or_replace_columns(*fields)
Table.rename_columns(*fields)
Table.drop_columns(*fields)
Table.explain()
Table.execute()
Table.execute_insert(target_path, overwrite=...)

// StatementSet
StatementSet.execute()
StatementSet.add_insert(target_path, table, overwrite=...)
StatementSet.add_insert_sql(stmt)

// TableResult
TableResult.get_job_client().cancel()
TableResult.wait(timeout_ms=...)
TableResult.collect()
TableResult.print()

// TableConfig
TableConfig.set(key, value)

// Expressions (confluent_pyflink.table.expressions)
col(), lit(), row(), and the other expression functions  # except call()

// Windows (confluent_pyflink.table.window)
Tumble.*
Slide.*
Session.*
Over.*

// Others
TableDescriptor.*
FormatDescriptor.*
```

Confluent adds the following classes for more convenience:
```text
ConfluentSettings.*
ConfluentTools.*
```

## Support

Table API goes hand in hand with Flink SQL on Confluent Cloud.
For feature requests or support tickets, use one of the [established channels](https://docs.confluent.io/cloud/current/flink/get-help.html).

### Frequent Issues

#### 1. `py4j.protocol.Py4JError: ConfluentSettings does not exist in the JVM`

This indicates that the Python API was unable to find a working Java runtime for starting a JVM process.

The plugin requires at least Java 11. Check that your `JAVA_HOME` environment variable is correctly set:
```
echo "$JAVA_HOME"
```

It should look similar to:
```java
/Users/Bob/.jenv/versions/11.0
```

Note: Only checking `java -version` might not be enough. It might be that it shows a correct Java version, but `JAVA_HOME`
still points to an invalid version. Consider using [jenv](https://github.com/jenv/jenv).

#### 2. `ConfluentSettingsValidationError: Missing the following required configuration keys: ...`

This indicates that something is wrong with your configuration. Make sure all required settings are provided as
`CONFLUENT_`-prefixed environment variables (a `.env` file works too), or via a JSON/YAML file passed to
`ConfluentSettings.from_file(...)`, as described above.
