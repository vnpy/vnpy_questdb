import importlib
import sys
import types
from datetime import datetime, timezone

import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import DB_TZ
from vnpy.trader.object import BarData, TickData
from vnpy.trader.setting import SETTINGS


# psycopg 与 questdb 未安装时，先放入不连接的模块，再导入数据库类。
def _prepare_questdb_drivers() -> None:
    try:
        importlib.import_module("psycopg")
    except Exception:
        for name in list(sys.modules):
            if name == "psycopg" or name.startswith("psycopg."):
                del sys.modules[name]
        rows: types.ModuleType = types.ModuleType("psycopg.rows")
        rows.DictRow = dict
        rows.dict_row = lambda _cursor: None
        psycopg: types.ModuleType = types.ModuleType("psycopg")
        psycopg.__path__ = []
        psycopg.rows = rows

        def _unpatched_connect(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("psycopg.connect was not patched")

        psycopg.connect = _unpatched_connect
        sys.modules["psycopg"] = psycopg
        sys.modules["psycopg.rows"] = rows

    try:
        importlib.import_module("questdb.ingress")
    except Exception:
        for name in list(sys.modules):
            if name == "questdb" or name.startswith("questdb."):
                del sys.modules[name]
        ingress: types.ModuleType = types.ModuleType("questdb.ingress")

        class _UnusedSender:
            @staticmethod
            def from_conf(_conf: str) -> object:
                raise AssertionError("Sender.from_conf was not patched")

        ingress.Sender = _UnusedSender
        questdb: types.ModuleType = types.ModuleType("questdb")
        questdb.__path__ = []
        questdb.ingress = ingress
        sys.modules["questdb"] = questdb
        sys.modules["questdb.ingress"] = ingress


_prepare_questdb_drivers()

SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 8812
SETTINGS["database.user"] = "admin"
SETTINGS["database.password"] = "quest"
SETTINGS["database.database"] = "qdb"
SETTINGS["database.http_port"] = 9000

from vnpy_questdb.questdb_database import (  # noqa: E402
    BAR_TABLE,
    CREATE_BAR_TABLE_SQL,
    CREATE_TICK_TABLE_SQL,
    LOAD_BAR_DATA_SQL,
    LOAD_TICK_DATA_SQL,
    QuestdbDatabase,
    SOFT_DELETE_BAR_DATA_SQL,
    TICK_TABLE,
)
import vnpy_questdb.questdb_database as qdb  # noqa: E402


sql_log: list[tuple[str, tuple[object, ...]]] = []
tuple_rows: list[tuple[object, ...]] = []
sender_calls: list["_Sender"] = []
init_sql: list[tuple[str, tuple[object, ...]]] = []


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        self.sql = sql
        stored: tuple[object, ...] = tuple(params) if params is not None else ()
        sql_log.append((sql, stored))

    def fetchmany(self, _size: int) -> list[tuple[object, ...]]:
        if not tuple_rows:
            return []
        rows: list[tuple[object, ...]] = list(tuple_rows)
        tuple_rows.clear()
        return rows

    def fetchone(self) -> dict[str, object] | None:
        if "count()" in self.sql:
            return {"count": 2}
        if "wal_tables" in self.sql:
            return {
                "suspended": False,
                "writerTxn": 1,
                "sequencerTxn": 1,
                "errorMessage": "",
            }
        return None


class _Connection:
    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def cursor(self) -> _Cursor:
        return _Cursor()


class _Sender:
    def __init__(self, conf: str) -> None:
        self.conf = conf
        self.rows: list[tuple[str, dict[str, str], dict[str, object], datetime]] = []
        self.flushed = False
        sender_calls.append(self)

    def __enter__(self) -> "_Sender":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    @staticmethod
    def from_conf(conf: str) -> "_Sender":
        return _Sender(conf)

    def row(
        self,
        table: str,
        symbols: dict[str, str],
        columns: dict[str, object],
        at: datetime,
    ) -> None:
        self.rows.append((table, symbols, columns, at))

    def flush(self) -> None:
        self.flushed = True


def _connect(conninfo: str, autocommit: bool = False, row_factory: object = None) -> _Connection:
    del conninfo, autocommit, row_factory
    return _Connection()


def _make_bars(symbol: str) -> list[BarData]:
    start: datetime = datetime(2024, 1, 15, 10, 0, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 1, 15, 10, 1, tzinfo=DB_TZ)
    return [
        BarData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=start,
            interval=Interval.MINUTE,
            volume=12.0,
            turnover=1.5,
            open_interest=3.0,
            open_price=100.0,
            high_price=110.0,
            low_price=90.0,
            close_price=105.0,
        ),
        BarData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=end,
            interval=Interval.MINUTE,
            volume=8.0,
            open_price=105.0,
            high_price=112.0,
            low_price=101.0,
            close_price=108.0,
        ),
    ]


def _make_ticks(symbol: str) -> list[TickData]:
    start: datetime = datetime(2024, 1, 16, 10, 0, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 1, 16, 10, 0, 1, tzinfo=DB_TZ)
    return [
        TickData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=start,
            name="au",
            last_price=400.5,
            volume=20.0,
            localtime=start,
        ),
        TickData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=end,
            name="au",
            last_price=401.0,
            volume=21.0,
            localtime=end,
        ),
    ]


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> QuestdbDatabase:
    sql_log.clear()
    tuple_rows.clear()
    sender_calls.clear()
    monkeypatch.setattr(qdb.psycopg, "connect", _connect)
    monkeypatch.setattr(qdb, "Sender", _Sender)
    db: QuestdbDatabase = QuestdbDatabase()
    init_sql[:] = list(sql_log)
    sql_log.clear()
    return db


def test_to_pg_datetime_is_naive_utc() -> None:
    local: datetime = datetime(2024, 1, 15, 10, 0, tzinfo=DB_TZ)
    converted: datetime = QuestdbDatabase._to_pg_datetime(local)
    assert converted.tzinfo is None
    assert converted == local.astimezone(timezone.utc).replace(tzinfo=None)


def test_from_questdb_datetime_treats_naive_as_utc() -> None:
    naive_utc: datetime = datetime(2024, 1, 15, 2, 0)
    converted: datetime = QuestdbDatabase._from_questdb_datetime(naive_utc)
    assert converted == naive_utc.replace(tzinfo=timezone.utc).astimezone(DB_TZ)

    aware: datetime = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
    assert QuestdbDatabase._from_questdb_datetime(aware) == aware.astimezone(DB_TZ)


def test_init_creates_tables_over_stubbed_connection(database: QuestdbDatabase) -> None:
    statements: list[str] = [sql for sql, _params in init_sql]
    assert CREATE_BAR_TABLE_SQL in statements
    assert CREATE_TICK_TABLE_SQL in statements
    assert database.ilp_conf == "http::addr=127.0.0.1:9000;"
    assert "host=127.0.0.1" in database.conninfo
    assert "port=8812" in database.conninfo
    assert "dbname=qdb" in database.conninfo


def test_save_bar_data_sends_ilp_row(database: QuestdbDatabase) -> None:
    symbol: str = "rb2405"
    bars: list[BarData] = _make_bars(symbol)
    assert database.save_bar_data(bars) is True
    assert len(sender_calls) == 1
    sender: _Sender = sender_calls[0]
    assert sender.conf == "http::addr=127.0.0.1:9000;"
    assert sender.flushed is True
    assert len(sender.rows) == 2

    table, symbols, columns, at = sender.rows[0]
    assert table == BAR_TABLE
    assert symbols == {
        "symbol": symbol,
        "exchange": Exchange.SHFE.value,
        "interval": Interval.MINUTE.value,
    }
    assert columns["volume"] == 12.0
    assert columns["turnover"] == 1.5
    assert columns["open_interest"] == 3.0
    assert columns["open_price"] == 100.0
    assert columns["high_price"] == 110.0
    assert columns["low_price"] == 90.0
    assert columns["close_price"] == 105.0
    assert columns["deleted"] is False
    assert at == bars[0].datetime.astimezone(timezone.utc)

    _table, second_symbols, second_columns, second_at = sender.rows[1]
    assert second_symbols["symbol"] == symbol
    assert second_columns["close_price"] == 108.0
    assert second_columns["volume"] == 8.0
    assert second_at == bars[1].datetime.astimezone(timezone.utc)

    wal: list[tuple[str, tuple[object, ...]]] = [
        item for item in sql_log if "wal_tables" in item[0]
    ]
    assert wal[-1][1] == (BAR_TABLE,)


def test_load_bar_data_uses_symbol_and_utc_bounds(database: QuestdbDatabase) -> None:
    symbol: str = "rb2405"
    start: datetime = datetime(2024, 1, 1, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 2, 1, tzinfo=DB_TZ)
    raw: datetime = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
    tuple_rows.append((raw, 12.0, 1.5, 3.0, 100.0, 110.0, 90.0, 105.0))

    loaded: list[BarData] = database.load_bar_data(
        symbol,
        Exchange.SHFE,
        Interval.MINUTE,
        start,
        end,
    )
    selects: list[tuple[str, tuple[object, ...]]] = [
        item for item in sql_log if item[0] == LOAD_BAR_DATA_SQL
    ]
    assert len(selects) == 1
    _sql, params = selects[0]
    assert params == (
        symbol,
        Exchange.SHFE.value,
        Interval.MINUTE.value,
        start.astimezone(timezone.utc).replace(tzinfo=None),
        end.astimezone(timezone.utc).replace(tzinfo=None),
    )
    assert len(loaded) == 1
    assert loaded[0].symbol == symbol
    assert loaded[0].exchange == Exchange.SHFE
    assert loaded[0].interval == Interval.MINUTE
    assert loaded[0].datetime == raw.astimezone(DB_TZ)
    assert loaded[0].open_price == 100.0
    assert loaded[0].high_price == 110.0
    assert loaded[0].low_price == 90.0
    assert loaded[0].close_price == 105.0
    assert loaded[0].volume == 12.0
    assert loaded[0].gateway_name == "DB"


def test_delete_bar_data_soft_deletes_by_key(database: QuestdbDatabase) -> None:
    count: int = database.delete_bar_data("rb2405", Exchange.SHFE, Interval.MINUTE)
    assert count == 2
    key: tuple[str, str, str] = ("rb2405", Exchange.SHFE.value, Interval.MINUTE.value)
    counted: list[tuple[str, tuple[object, ...]]] = [
        item for item in sql_log if "count()" in item[0]
    ]
    deleted: list[tuple[str, tuple[object, ...]]] = [
        item for item in sql_log if item[0] == SOFT_DELETE_BAR_DATA_SQL
    ]
    assert len(counted) == 1
    assert counted[0][1] == key
    assert len(deleted) == 1
    assert deleted[0][1] == key
    assert "deleted = true" in deleted[0][0]


def test_save_tick_data_sends_symbol_exchange_and_last_price(database: QuestdbDatabase) -> None:
    symbol: str = "au2406"
    ticks: list[TickData] = _make_ticks(symbol)
    assert database.save_tick_data(ticks) is True
    assert len(sender_calls) == 1
    sender: _Sender = sender_calls[0]
    assert sender.flushed is True
    assert len(sender.rows) == 2
    table, symbols, columns, at = sender.rows[0]
    assert table == TICK_TABLE
    assert symbols == {"symbol": symbol, "exchange": Exchange.SHFE.value}
    assert columns["name"] == "au"
    assert columns["last_price"] == 400.5
    assert columns["volume"] == 20.0
    assert columns["deleted"] is False
    assert columns["localtime"] == ticks[0].localtime.astimezone(timezone.utc)
    assert at == ticks[0].datetime.astimezone(timezone.utc)

    _table, _symbols, second_columns, second_at = sender.rows[1]
    assert second_columns["last_price"] == 401.0
    assert second_at == ticks[1].datetime.astimezone(timezone.utc)

    start: datetime = datetime(2024, 1, 1, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 2, 1, tzinfo=DB_TZ)
    sql_log.clear()
    loaded: list[TickData] = database.load_tick_data(symbol, Exchange.SHFE, start, end)
    assert loaded == []
    selects: list[tuple[str, tuple[object, ...]]] = [
        item for item in sql_log if item[0] == LOAD_TICK_DATA_SQL
    ]
    assert len(selects) == 1
    assert selects[0][1] == (
        symbol,
        Exchange.SHFE.value,
        start.astimezone(timezone.utc).replace(tzinfo=None),
        end.astimezone(timezone.utc).replace(tzinfo=None),
    )


def test_empty_bar_list_skips_sender(database: QuestdbDatabase) -> None:
    assert database.save_bar_data([]) is True
    assert sender_calls == []


def test_save_bar_data_rejects_missing_interval(database: QuestdbDatabase) -> None:
    bar: BarData = _make_bars("rb2405")[0]
    bar.interval = None
    with pytest.raises(ValueError, match="interval"):
        database.save_bar_data([bar])
