from dataclasses import dataclass
from typing import Callable


@dataclass
class Config:
    add_type_ignore: bool = False
    include_name: Callable[[str], bool] = lambda _: True
    drop_unused_enums: bool = True
    detect_enum_values_changes: bool = True


_config = Config()


def set_configuration(config: Config):
    global _config
    _config = config


def get_configuration() -> Config:
    return _config
