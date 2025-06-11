from dataclasses import dataclass
from typing import List
import yaml


@dataclass
class TelegramConfig:
    api_id: str
    api_hash: str
    bot_token: str


@dataclass
class AdminConfig:
    sysadmin_ids: List[int]


@dataclass
class QueueConfig:
    type: str
    host: str
    port: int
    username: str
    password: str
    vhost: str


@dataclass
class UiConfig:
    page_size: int


@dataclass
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str


@dataclass
class Config:
    telegram: TelegramConfig
    admin: AdminConfig
    queue: QueueConfig
    ui: UiConfig
    postgres: PostgresConfig

    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(
            telegram=TelegramConfig(**data['telegram']),
            admin=AdminConfig(**data['admin']),
            queue=QueueConfig(**data['queue']),
            ui=UiConfig(**data['ui']),
            postgres=PostgresConfig(**data['postgres'])
        ) 