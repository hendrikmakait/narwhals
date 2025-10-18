from __future__ import annotations

from typing import TYPE_CHECKING, Any

import datafusion

from narwhals._arrow.utils import native_to_narwhals_dtype
from narwhals._datafusion.utils import catch_datafusion_exception, evaluate_exprs
from narwhals._sql.dataframe import SQLLazyFrame
from narwhals._utils import ValidateBackendVersion, Version

if TYPE_CHECKING:
    from typing_extensions import Self, TypeIs

    from narwhals._datafusion.expr import DataFusionExpr
    from narwhals._utils import _LimitedContext
    from narwhals.dtypes import DType


class DataFusionLazyFrame(
    SQLLazyFrame[
        "DataFusionExpr",
        "datafusion.DataFrame",
        "LazyFrame[datafusion.DataFrame] | DataFrameV1[datafusion.DataFrame]",
    ],
    ValidateBackendVersion,
):
    def __init__(
        self,
        df: datafusion.DataFrame,
        *,
        version: Version,
        validate_backend_version: bool = False,
    ) -> None:
        self._native_frame: datafusion.DataFrame = df
        self._version = version
        if validate_backend_version:
            self._validate_backend_version()

    @staticmethod
    def _is_native(obj: datafusion.DataFrame | Any) -> TypeIs[datafusion.DataFrame]:
        return isinstance(obj, datafusion.DataFrame)

    @classmethod
    def from_native(
        cls, data: datafusion.DataFrame, /, *, context: _LimitedContext
    ) -> Self:
        return cls(data, version=context._version)

    @property
    def schema(self) -> dict[str, DType]:
        return {
            field.name: native_to_narwhals_dtype(field.type, self._version)
            for field in self.native.schema()
        }

    def collect_schema(self) -> dict[str, DType]:
        return self.schema

    def select(self, *exprs: DataFusionExpr) -> Self:
        new_columns = evaluate_exprs(self, *exprs)
        new_columns_list = [col.alias(col_name) for (col_name, col) in new_columns]
        if self._implementation.is_pyspark():  # pragma: no cover
            try:
                return self._with_native(self.native.select(*new_columns_list))
            except Exception as e:  # noqa: BLE001
                raise catch_datafusion_exception(e, self) from None
        return self._with_native(self.native.select(*new_columns_list))
