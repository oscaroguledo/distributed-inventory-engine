import os

# Must be set before order_api.main (and its OTel setup) is ever imported —
# a root conftest.py is guaranteed by pytest to load before any test module.
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
