from order_api.core.config import Settings, get_settings


def test_default_values_ignore_any_real_env_file():
    settings = Settings(_env_file=None)

    assert settings.service_name == "order-api"
    assert settings.log_level == "INFO"
    assert settings.postgres_url.startswith("postgresql+asyncpg://")
    assert settings.redis_url.startswith("redis://")
    assert settings.hold_ttl_seconds == 900


def test_get_settings_is_cached():
    assert get_settings() is get_settings()


def test_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    get_settings.cache_clear()

    assert get_settings().log_level == "DEBUG"

    monkeypatch.delenv("LOG_LEVEL", raising=False)
    get_settings.cache_clear()
