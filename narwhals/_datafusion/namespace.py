from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion.expr import Expr

from narwhals._datafusion.dataframe import DataFusionLazyFrame
from narwhals._datafusion.expr import DataFusionExpr
from narwhals._sql.namespace import SQLNamespace
from narwhals._utils import Implementation

if TYPE_CHECKING:
    from narwhals._utils import Version


class DataFusionNamespace(
    SQLNamespace[DataFusionLazyFrame, DataFusionExpr, "datafusion.DataFrame", Expr]
):
    _implementation: Implementation = Implementation.DATAFUSION

    def __init__(self, *, version: Version) -> None:
        self._version = version

    @property
    def _expr(self) -> type[DataFusionExpr]:
        return DataFusionExpr

    @property
    def _lazyframe(self) -> type[DataFusionLazyFrame]:
        return DataFusionLazyFrame
