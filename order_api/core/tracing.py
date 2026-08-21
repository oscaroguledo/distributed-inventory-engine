import logging
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger("order_api.tracing")


def setup_tracing(service_name: str, otlp_endpoint: str) -> None:
    """Exports spans via OTLP/gRPC to Jaeger. OTEL_SDK_DISABLED=true skips
    this — instrumented code falls back to the OTel API's built-in no-op."""
    if os.environ.get("OTEL_SDK_DISABLED", "").lower() == "true":
        logger.info("tracing disabled via OTEL_SDK_DISABLED")
        return
    _configure_provider(service_name, otlp_endpoint)  # pragma: no cover -- live Docker only


def _configure_provider(name: str, endpoint: str) -> None:  # pragma: no cover -- live Docker only
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: name}))
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logger.info("tracing configured: service=%s endpoint=%s", name, endpoint)
