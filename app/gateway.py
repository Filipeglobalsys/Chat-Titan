import asyncio
from sqlalchemy import create_engine, text, inspect as sa_inspect

_gateway_configs: dict[str, str] = {}  # dataset_id -> connection_string


def set_gateway_config(dataset_id: str, connection_string: str) -> None:
    _gateway_configs[dataset_id] = connection_string


def get_gateway_config(dataset_id: str) -> str | None:
    return _gateway_configs.get(dataset_id)


def build_connection_string(
    dialect: str, host: str, port: int | None, db_name: str, user: str, password: str
) -> str:
    defaults = {"mssql": 1433, "postgresql": 5432, "mysql": 3306}
    p = port or defaults.get(dialect, 1433)
    import urllib.parse
    pw = urllib.parse.quote_plus(password)
    usr = urllib.parse.quote_plus(user)
    if dialect == "postgresql":
        return f"postgresql+psycopg2://{usr}:{pw}@{host}:{p}/{db_name}"
    if dialect == "mysql":
        return f"mysql+pymysql://{usr}:{pw}@{host}:{p}/{db_name}"
    return f"mssql+pymssql://{usr}:{pw}@{host}:{p}/{db_name}"


def detect_dialect(conn_str: str) -> str:
    cs = conn_str.lower()
    if cs.startswith("mssql") or "pymssql" in cs or "pyodbc" in cs:
        return "mssql"
    if cs.startswith("postgresql") or cs.startswith("postgres"):
        return "postgresql"
    if cs.startswith("mysql"):
        return "mysql"
    return "sql"


def _read_schema_sync(connection_string: str) -> list:
    engine = create_engine(connection_string, connect_args={"timeout": 15})
    inspector = sa_inspect(engine)
    tables = []
    for table_name in inspector.get_table_names():
        try:
            cols = inspector.get_columns(table_name)
        except Exception:
            continue
        columns = [{"name": c["name"], "dataType": str(c["type"]), "columnType": "Column"} for c in cols]
        tables.append({"name": table_name, "columns": columns, "measures": []})
    engine.dispose()
    return tables


async def get_gateway_schema(connection_string: str) -> list:
    return await asyncio.to_thread(_read_schema_sync, connection_string)


def _execute_sql_sync(connection_string: str, sql: str) -> list:
    engine = create_engine(connection_string, connect_args={"timeout": 30})
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        cols = list(result.keys())
        rows = [dict(zip(cols, row)) for row in result.fetchmany(500)]
    engine.dispose()
    return rows


async def execute_sql(connection_string: str, sql: str) -> list:
    return await asyncio.to_thread(_execute_sql_sync, connection_string, sql)
