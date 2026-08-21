from order_api.core.tracing import setup_tracing


def test_setup_tracing_is_a_noop_when_sdk_disabled(monkeypatch, caplog):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")

    with caplog.at_level("INFO"):
        setup_tracing("test-service", "localhost:4317")

    assert any("tracing disabled" in record.message for record in caplog.records)


def test_setup_tracing_disabled_check_is_case_insensitive(monkeypatch, caplog):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "TRUE")

    with caplog.at_level("INFO"):
        setup_tracing("test-service", "localhost:4317")

    assert any("tracing disabled" in record.message for record in caplog.records)
