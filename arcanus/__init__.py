# This file makes Python treat the `arcanus` directory as a package.

# Placeholder for arcanus.config, which will be created in a subsequent step.
# For now, to allow spark_session.py to import it without error,
# we can define dummy variables or leave it empty if spark_session.py
# is robust enough (which it should be if it's only accessing attributes).

# Example of defining dummy attributes if needed by other modules during development:
# SPARK_APP_NAME = "ArcanusPrime_Dev"
# SPARK_MASTER = "local[*]"

# We will populate this properly when arcanus/config.py is created.
# For now, spark_session.py will attempt to import `config` from this package.
# If config.py doesn't exist yet or doesn't have the attributes,
# an AttributeError will occur when get_spark_session() is called and tries to access config.SPARK_APP_NAME.
# This is acceptable for now as the next step is to create config.py.

print("INFO: arcanus/__init__.py loaded. Awaiting arcanus/config.py for Spark configuration.")
