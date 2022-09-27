from __future__ import annotations

from dbt.adapters.python.impl import PythonAdapter
from dbt.contracts.connection import AdapterResponse
from .connections import FalConnectionManager
from .utils import retrieve_symbol
from .adapter_support import write_to_relation, read_relation_as_df, prepare_for_adapter


class FalAdapter(PythonAdapter):
    ConnectionManager = FalConnectionManager

    def submit_python_job(
        self, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        """Execute the given `compiled_code` in the target environment."""

        environment = parsed_model["config"].get("fal_environment", "local")

        # Fal generates main function as our entry-point, where it expects
        # us to pass a couple of parameters.
        main = retrieve_symbol(compiled_code, "main")
        return main(
            read_df=prepare_for_adapter(self._db_adapter, read_relation_as_df),
            write_df=prepare_for_adapter(self._db_adapter, write_to_relation),
        )

    @classmethod
    def type(cls):
        return "fal"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False
