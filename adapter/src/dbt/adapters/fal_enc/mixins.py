from dbt.adapters.fal.impl import FalAdapterMixin
from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.base.impl import BaseAdapter

class FalTypeCredentialMixin():
    @property
    def type(self):
        return "fal_enc"


class FalEncAdapterMixin(FalAdapterMixin):
    def __init__(self, config, db_adapter_type: BaseAdapter):
        # Use the db_adapter connection manager
        self.ConnectionManager = db_adapter_type.ConnectionManager

        db_adapter = get_adapter_by_type(db_adapter_type.type())
        super().__init__(config, db_adapter)

        # HACK: A Python adapter does not have _available_ all the attributes a DB adapter does.
        # Since we use the DB adapter as the storage for the Python adapter, we must proxy to it
        # all the unhandled calls.
        self._available_ = self._db_adapter._available_.union(self._available_)

    def submit_python_job(self, *args, **kwargs):
        return super().submit_python_job(*args, **kwargs)

    @classmethod
    def type(cls):
        return "fal_enc"

    def __getattr__(self, name):
        """
        Directly proxy to the DB adapter, Python adapter in this case does what we explicitly define in this class.
        """
        if hasattr(self._db_adapter, name):
            return getattr(self._db_adapter, name)
        else:
            getattr(super(), name)
