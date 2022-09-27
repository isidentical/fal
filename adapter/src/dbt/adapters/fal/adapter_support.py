import pandas as pd
import sqlalchemy
import functools
from dbt.adapters.base import BaseAdapter, BaseRelation, RelationType
from dbt.adapters.base.connections import AdapterResponse, Connection
from typing import Any

_SQLALCHEMY_DIALECTS = {
    "postgres": "postgresql+psycopg2",
    "redshift": "redshift+psycopg2",
}


def _get_alchemy_engine(adapter: BaseAdapter) -> Any:
    # This function expects the caller to already manage
    # the connection.
    connection = adapter.connections.get_thread_connection()

    # The following code heavily depends on the implementation
    # details of the known adapters, hence it can't work for
    # arbitrary ones.
    adapter_type = adapter.type()

    sqlalchemy_kwargs = {}
    if adapter_type in ("postgres", "redshift", "snowflake", "duckdb"):
        # If the given adapter supports the DBAPI (PEP 249), we can
        # use its connection directly for the engine.
        sqlalchemy_kwargs["creator"] = lambda: connection.handle
    elif adapter_type == "bigquery":
        # BigQuery's connection object returns a google-cloud
        # client, which doesn't directly support the DBAPI but
        # we can still partially leverage it to construct the
        # engine object.
        sqlalchemy_kwargs["connect_args"] = {"client": connection.handle}
    else:
        # TODO: maybe tell them to open an issue?
        raise NotImplementedError(
            "dbt-fal does not support the given adapter " "for materializing relations."
        )

    url = _SQLALCHEMY_DIALECTS.get(adapter_type, adapter_type) + "://"
    return sqlalchemy.create_engine(url, **sqlalchemy_kwargs)


def drop_relation_if_it_exists(adapter: BaseAdapter, relation: BaseRelation) -> None:
    if adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier,
    ):
        adapter.drop_relation(relation)


def write_to_relation(
    adapter: BaseAdapter,
    relation: BaseRelation,
    dataframe: pd.DataFrame,
    *,
    if_exists: str = "replace",
) -> AdapterResponse:
    """Generic version of the write_df_to_relation. Materialize the given
    dataframe to the targeted relation on the adapter."""

    with adapter.connection_named("fal:write_to_relation"):
        # TODO: this should probably live in the materialization macro.
        temp_relation = relation.replace_path(
            identifier=f"__dbt_fal_temp_{relation.identifier}"
        )
        drop_relation_if_it_exists(adapter, temp_relation)

        alchemy_engine = _get_alchemy_engine(adapter)

        # TODO: probably worth handling errors here an returning
        # a proper adapter response.
        rows_affected = dataframe.to_sql(
            con=alchemy_engine,
            name=temp_relation.identifier,
            schema=temp_relation.schema,
            if_exists=if_exists,
            index=False,
        )
        adapter.cache.add(temp_relation)
        drop_relation_if_it_exists(adapter, relation)
        adapter.rename_relation(temp_relation, relation)
        adapter.commit_if_has_connection()

    return AdapterResponse("OK", rows_affected=rows_affected)


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    """Generic version of the read_df_from_relation."""

    with adapter.connection_named("fal:read_relation_as_df"):
        alchemy_engine = _get_alchemy_engine(adapter)
        return pd.read_sql_table(
            con=alchemy_engine,
            table_name=relation.identifier,
            schema=relation.schema,
        )


def prepare_for_adapter(adapter: BaseAdapter, function: Any) -> Any:
    """Prepare the given function to be used with string-like inputs
    (for relations) on the given adapter."""

    @functools.wraps(function)
    def wrapped(quoted_relation: str, *args, **kwargs) -> Any:
        relation = adapter.Relation.create(*quoted_relation.split("."), type=RelationType.Table)
        return function(adapter, relation, *args, **kwargs)

    return wrapped
