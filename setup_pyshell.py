from confluent_pyflink.table.utils import *
from confluent_pyflink.table import *
from confluent_pyflink.table.expressions import *

settings = ConfluentSettings()
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
print()
print()
