from __future__ import annotations

from narwhals._sql.expr import SQLExpr
from narwhals._utils import Implementation


class DataFusionExpr(SQLExpr["DataFusionLazyFrame", "Expr"]):
    _implementation = Implementation.DATAFUSION
