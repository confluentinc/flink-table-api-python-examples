from pyflink.table.confluent import *
from pyflink.table import *
from pyflink.table.expressions import *

settings = ConfluentSettings.from_global_variables()
env = TableEnvironment.create(settings)

print()
print()
print("Welcome to Apache Flink® Table API on Confluent Cloud")
print()
print()
print("A TableEnvironment has been pre-initialized and is available under `env`.")
print()
print("Some inspirations to get started:")
print("  - Say hello: env.execute_sql(\"SELECT 'Hello world!'\").print()")
print("  - List catalogs: env.list_catalogs()")
print("  - Show something fancy: env.from_path(\"examples.marketplace.clicks\").execute().print()")
print()
print()
