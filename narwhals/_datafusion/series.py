from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._arrow.utils import native_to_narwhals_dtype
from narwhals.dependencies import get_datafusion

if TYPE_CHECKING:
    from types import ModuleType

    import datafusion
    from typing_extensions import Never, Self

    from narwhals._utils import Version
    from narwhals.dtypes import DType


class DataFusionInterchangeSeries:
    def __init__(self, df: datafusion.DataFrame, version: Version) -> None:
        self._native_series = df
        self._version = version

    def __narwhals_series__(self) -> Self:
        return self

    def __native_namespace__(self) -> ModuleType:
        return get_datafusion()

    @property
    def dtype(self) -> DType:
        return native_to_narwhals_dtype(
            self._native_series.schema().types[0], self._version
        )

    def __getattr__(self, attr: str) -> Never:
        msg = (  # pragma: no cover
            f"Attribute {attr} is not supported for interchange-level dataframes.\n\n"
            "If you would like to see this kind of object better supported in "
            "Narwhals, please open a feature request "
            "at https://github.com/narwhals-dev/narwhals/issues."
        )
        raise NotImplementedError(msg)  # pragma: no cover
