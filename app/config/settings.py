"""Application settings using Pydantic.

Field names are matched to environment variables case-insensitively by
pydantic-settings (e.g. `postgres_host` <-> `POSTGRES_HOST`) -- no explicit
`env=` alias needed per field.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "asyncflow"
    postgres_password: str = "change_me"
    postgres_db: str = "asyncflow"

    # RabbitMQ
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_http_port: int = 15672

    # Interface the `asyncflow-api` console script listens on. Loopback by default;
    # the API has no authentication, so set API_HOST=0.0.0.0 only on a trusted network.
    api_host: str = "127.0.0.1"

    # AsyncFlow settings
    # Despite the name, this is the TOTAL number of attempts a task gets
    # (stored per task as tasks.max_attempts): 5 -> 1 first try + 4 retries.
    max_task_retries: int = 5
    retry_backoff_base_seconds: float = 2.0
    retry_backoff_max_seconds: float = 60.0
    worker_concurrency: int = 5
    gracefully_shutdown_timeout_seconds: int = 30

    # Derived DSNs
    @property
    def postgres_dsn(self) -> str:
        """Async DSN (asyncpg driver) used by the running application."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_dsn_sync(self) -> str:
        """Plain DSN (no driver prefix) used by tools that shell out to psql."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def rabbitmq_dsn(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}/"
        )


settings = Settings()
