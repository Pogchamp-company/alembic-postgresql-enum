from dataclasses import dataclass
from enum import Enum as PyEnum
from typing import Union, Tuple, Dict, FrozenSet

from sqlalchemy import Enum, ARRAY


class ColumnType(PyEnum):
    COMMON = Enum
    ARRAY = ARRAY

    def __repr__(self):
        return f'{self.__class__.__name__}.{self.name}'


@dataclass(frozen=True)
class TableReference:
    table_name: str
    column_name: str
    column_type: ColumnType = ColumnType.COMMON

    def to_tuple(self) -> Union[Tuple[str, str], Tuple[str, str, ColumnType]]:
        if self.column_type == ColumnType.COMMON:
            return self.table_name, self.column_name
        return self.table_name, self.column_name, self.column_type


EnumNamesToValues = Dict[str, Tuple[str, ...]]
EnumNamesToTableReferences = Dict[str, FrozenSet[TableReference]]


@dataclass
class DeclaredEnumValues:
    enum_values: EnumNamesToValues
    enum_table_references: EnumNamesToTableReferences
