from datetime import datetime, timezone
from time import monotonic, sleep
from typing import Any

import psycopg
from psycopg.rows import DictRow, dict_row
from questdb.ingress import Sender

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    TickOverview,
    convert_tz,
)
from vnpy.trader.object import BarData, TickData
from vnpy.trader.setting import SETTINGS


BAR_TABLE: str = "dbbardata"
TICK_TABLE: str = "dbtickdata"
FETCH_SIZE: int = 10_000

BAR_DDL: str = f"""
CREATE TABLE IF NOT EXISTS {BAR_TABLE} (
    symbol SYMBOL CAPACITY 256 CACHE,
    exchange SYMBOL CAPACITY 32 CACHE,
    interval SYMBOL CAPACITY 16 CACHE,
    datetime TIMESTAMP,
    volume DOUBLE,
    turnover DOUBLE,
    open_interest DOUBLE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    deleted BOOLEAN
) TIMESTAMP(datetime)
PARTITION BY MONTH
WAL
DEDUP UPSERT KEYS(datetime, symbol, exchange, interval);
"""

TICK_DDL: str = f"""
CREATE TABLE IF NOT EXISTS {TICK_TABLE} (
    symbol SYMBOL CAPACITY 256 CACHE,
    exchange SYMBOL CAPACITY 32 CACHE,
    datetime TIMESTAMP,
    name STRING,
    volume DOUBLE,
    turnover DOUBLE,
    open_interest DOUBLE,
    last_price DOUBLE,
    last_volume DOUBLE,
    limit_up DOUBLE,
    limit_down DOUBLE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    pre_close DOUBLE,
    bid_price_1 DOUBLE,
    bid_price_2 DOUBLE,
    bid_price_3 DOUBLE,
    bid_price_4 DOUBLE,
    bid_price_5 DOUBLE,
    ask_price_1 DOUBLE,
    ask_price_2 DOUBLE,
    ask_price_3 DOUBLE,
    ask_price_4 DOUBLE,
    ask_price_5 DOUBLE,
    bid_volume_1 DOUBLE,
    bid_volume_2 DOUBLE,
    bid_volume_3 DOUBLE,
    bid_volume_4 DOUBLE,
    bid_volume_5 DOUBLE,
    ask_volume_1 DOUBLE,
    ask_volume_2 DOUBLE,
    ask_volume_3 DOUBLE,
    ask_volume_4 DOUBLE,
    ask_volume_5 DOUBLE,
    localtime TIMESTAMP,
    deleted BOOLEAN
) TIMESTAMP(datetime)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(datetime, symbol, exchange);
"""


class QuestdbDatabase(BaseDatabase):
    """QuestDB数据库接口"""

    def __init__(self) -> None:
        """"""
        self.host: str = str(SETTINGS.get("database.host", "localhost"))
        self.port: int = int(SETTINGS.get("database.port", 8812))
        self.user: str = str(SETTINGS.get("database.user", "admin"))
        self.password: str = str(SETTINGS.get("database.password", "quest"))
        self.database: str = str(SETTINGS.get("database.database", "qdb"))

        self.http_port: int = int(SETTINGS.get("database.http_port", 9000))
        self.tcp_port: int = int(SETTINGS.get("database.tcp_port", 9009))
        self.ilp_protocol: str = str(SETTINGS.get("database.ilp_protocol", "http")).lower()
        self.auto_flush_rows: int = int(SETTINGS.get("database.auto_flush_rows", 75_000))
        self.auto_flush_interval: int = int(SETTINGS.get("database.auto_flush_interval", 1_000))
        self.request_timeout: int = int(SETTINGS.get("database.request_timeout", 30_000))
        self.retry_timeout: int = int(SETTINGS.get("database.retry_timeout", 10_000))
        self.wal_apply_timeout: float = float(SETTINGS.get("database.wal_apply_timeout", 30))

        self.conninfo: str = (
            f"host={self.host} "
            f"port={self.port} "
            f"user={self.user} "
            f"password={self.password} "
            f"dbname={self.database}"
        )
        self.ilp_conf: str = self._create_ilp_conf()

        self.init_tables()

    def _create_ilp_conf(self) -> str:
        """创建QuestDB ILP客户端配置"""
        if self.ilp_protocol == "tcp":
            return f"tcp::addr={self.host}:{self.tcp_port};protocol_version=2;"

        return (
            f"http::addr={self.host}:{self.http_port};"
            f"auto_flush_rows={self.auto_flush_rows};"
            f"auto_flush_interval={self.auto_flush_interval};"
            f"request_timeout={self.request_timeout};"
            f"retry_timeout={self.retry_timeout};"
        )

    def init_tables(self) -> None:
        """初始化数据库表"""
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(BAR_DDL)
                cursor.execute(TICK_DDL)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        if not bars:
            return True

        with Sender.from_conf(self.ilp_conf) as sender:
            for bar in bars:
                interval: Interval | None = bar.interval
                if interval is None:
                    raise ValueError("BarData.interval不能为空")

                sender.row(
                    BAR_TABLE,
                    symbols={
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "interval": interval.value,
                    },
                    columns={
                        "volume": bar.volume,
                        "turnover": bar.turnover,
                        "open_interest": bar.open_interest,
                        "open_price": bar.open_price,
                        "high_price": bar.high_price,
                        "low_price": bar.low_price,
                        "close_price": bar.close_price,
                        "deleted": False,
                    },
                    at=self._to_questdb_datetime(bar.datetime),
                )
            sender.flush()

        self._wait_wal_apply(BAR_TABLE)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        if not ticks:
            return True

        with Sender.from_conf(self.ilp_conf) as sender:
            for tick in ticks:
                columns: dict[str, Any] = {
                    "name": tick.name,
                    "volume": tick.volume,
                    "turnover": tick.turnover,
                    "open_interest": tick.open_interest,
                    "last_price": tick.last_price,
                    "last_volume": tick.last_volume,
                    "limit_up": tick.limit_up,
                    "limit_down": tick.limit_down,
                    "open_price": tick.open_price,
                    "high_price": tick.high_price,
                    "low_price": tick.low_price,
                    "pre_close": tick.pre_close,
                    "bid_price_1": tick.bid_price_1,
                    "bid_price_2": tick.bid_price_2,
                    "bid_price_3": tick.bid_price_3,
                    "bid_price_4": tick.bid_price_4,
                    "bid_price_5": tick.bid_price_5,
                    "ask_price_1": tick.ask_price_1,
                    "ask_price_2": tick.ask_price_2,
                    "ask_price_3": tick.ask_price_3,
                    "ask_price_4": tick.ask_price_4,
                    "ask_price_5": tick.ask_price_5,
                    "bid_volume_1": tick.bid_volume_1,
                    "bid_volume_2": tick.bid_volume_2,
                    "bid_volume_3": tick.bid_volume_3,
                    "bid_volume_4": tick.bid_volume_4,
                    "bid_volume_5": tick.bid_volume_5,
                    "ask_volume_1": tick.ask_volume_1,
                    "ask_volume_2": tick.ask_volume_2,
                    "ask_volume_3": tick.ask_volume_3,
                    "ask_volume_4": tick.ask_volume_4,
                    "ask_volume_5": tick.ask_volume_5,
                    "deleted": False,
                }

                if tick.localtime:
                    columns["localtime"] = self._to_questdb_datetime(tick.localtime)

                sender.row(
                    TICK_TABLE,
                    symbols={
                        "symbol": tick.symbol,
                        "exchange": tick.exchange.value,
                    },
                    columns=columns,
                    at=self._to_questdb_datetime(tick.datetime),
                )
            sender.flush()

        self._wait_wal_apply(TICK_TABLE)

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        sql: str = f"""
            SELECT
                symbol,
                exchange,
                interval,
                datetime,
                volume,
                turnover,
                open_interest,
                open_price,
                high_price,
                low_price,
                close_price
            FROM {BAR_TABLE}
            WHERE symbol = %s
              AND exchange = %s
              AND interval = %s
              AND datetime >= %s
              AND datetime <= %s
              AND (deleted = false OR deleted IS NULL)
            ORDER BY datetime;
        """

        params: tuple[Any, ...] = (
            symbol,
            exchange.value,
            interval.value,
            self._to_pg_datetime(start),
            self._to_pg_datetime(end),
        )

        bars: list[BarData] = []
        for row in self._iter_rows(sql, params):
            bar: BarData = BarData(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                datetime=self._from_questdb_datetime(row["datetime"]),
                interval=Interval(row["interval"]),
                volume=row["volume"],
                turnover=row["turnover"],
                open_interest=row["open_interest"],
                open_price=row["open_price"],
                high_price=row["high_price"],
                low_price=row["low_price"],
                close_price=row["close_price"],
                gateway_name="DB",
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取TICK数据"""
        sql: str = f"""
            SELECT
                symbol,
                exchange,
                datetime,
                name,
                volume,
                turnover,
                open_interest,
                last_price,
                last_volume,
                limit_up,
                limit_down,
                open_price,
                high_price,
                low_price,
                pre_close,
                bid_price_1,
                bid_price_2,
                bid_price_3,
                bid_price_4,
                bid_price_5,
                ask_price_1,
                ask_price_2,
                ask_price_3,
                ask_price_4,
                ask_price_5,
                bid_volume_1,
                bid_volume_2,
                bid_volume_3,
                bid_volume_4,
                bid_volume_5,
                ask_volume_1,
                ask_volume_2,
                ask_volume_3,
                ask_volume_4,
                ask_volume_5,
                localtime
            FROM {TICK_TABLE}
            WHERE symbol = %s
              AND exchange = %s
              AND datetime >= %s
              AND datetime <= %s
              AND (deleted = false OR deleted IS NULL)
            ORDER BY datetime;
        """

        params: tuple[Any, ...] = (
            symbol,
            exchange.value,
            self._to_pg_datetime(start),
            self._to_pg_datetime(end),
        )

        ticks: list[TickData] = []
        for row in self._iter_rows(sql, params):
            localtime: datetime | None = None
            if row["localtime"]:
                localtime = self._from_questdb_datetime(row["localtime"])

            tick: TickData = TickData(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                datetime=self._from_questdb_datetime(row["datetime"]),
                name=row["name"],
                volume=row["volume"],
                turnover=row["turnover"],
                open_interest=row["open_interest"],
                last_price=row["last_price"],
                last_volume=row["last_volume"],
                limit_up=row["limit_up"],
                limit_down=row["limit_down"],
                open_price=row["open_price"],
                high_price=row["high_price"],
                low_price=row["low_price"],
                pre_close=row["pre_close"],
                bid_price_1=row["bid_price_1"],
                bid_price_2=row["bid_price_2"],
                bid_price_3=row["bid_price_3"],
                bid_price_4=row["bid_price_4"],
                bid_price_5=row["bid_price_5"],
                ask_price_1=row["ask_price_1"],
                ask_price_2=row["ask_price_2"],
                ask_price_3=row["ask_price_3"],
                ask_price_4=row["ask_price_4"],
                ask_price_5=row["ask_price_5"],
                bid_volume_1=row["bid_volume_1"],
                bid_volume_2=row["bid_volume_2"],
                bid_volume_3=row["bid_volume_3"],
                bid_volume_4=row["bid_volume_4"],
                bid_volume_5=row["bid_volume_5"],
                ask_volume_1=row["ask_volume_1"],
                ask_volume_2=row["ask_volume_2"],
                ask_volume_3=row["ask_volume_3"],
                ask_volume_4=row["ask_volume_4"],
                ask_volume_5=row["ask_volume_5"],
                localtime=localtime,
                gateway_name="DB",
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        count_sql: str = f"""
            SELECT count() AS count
            FROM {BAR_TABLE}
            WHERE symbol = %s
              AND exchange = %s
              AND interval = %s
              AND (deleted = false OR deleted IS NULL);
        """
        update_sql: str = f"""
            UPDATE {BAR_TABLE}
            SET deleted = true
            WHERE symbol = %s
              AND exchange = %s
              AND interval = %s
              AND (deleted = false OR deleted IS NULL);
        """
        params: tuple[Any, ...] = (symbol, exchange.value, interval.value)

        count: int = self._query_count(count_sql, params)
        self._execute(update_sql, params)
        self._wait_wal_apply(BAR_TABLE)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        count_sql: str = f"""
            SELECT count() AS count
            FROM {TICK_TABLE}
            WHERE symbol = %s
              AND exchange = %s
              AND (deleted = false OR deleted IS NULL);
        """
        update_sql: str = f"""
            UPDATE {TICK_TABLE}
            SET deleted = true
            WHERE symbol = %s
              AND exchange = %s
              AND (deleted = false OR deleted IS NULL);
        """
        params: tuple[Any, ...] = (symbol, exchange.value)

        count: int = self._query_count(count_sql, params)
        self._execute(update_sql, params)
        self._wait_wal_apply(TICK_TABLE)

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询数据库中的K线汇总信息"""
        sql: str = f"""
            SELECT
                symbol,
                exchange,
                interval,
                count() AS count,
                min(datetime) AS start_datetime,
                max(datetime) AS end_datetime
            FROM {BAR_TABLE}
            WHERE deleted = false OR deleted IS NULL
            GROUP BY symbol, exchange, interval
            ORDER BY symbol, exchange, interval;
        """

        overviews: list[BarOverview] = []
        for row in self._iter_rows(sql):
            overview: BarOverview = BarOverview(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                interval=Interval(row["interval"]),
                count=int(row["count"]),
                start=self._from_questdb_datetime(row["start_datetime"]),
                end=self._from_questdb_datetime(row["end_datetime"]),
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """查询数据库中的Tick汇总信息"""
        sql: str = f"""
            SELECT
                symbol,
                exchange,
                count() AS count,
                min(datetime) AS start_datetime,
                max(datetime) AS end_datetime
            FROM {TICK_TABLE}
            WHERE deleted = false OR deleted IS NULL
            GROUP BY symbol, exchange
            ORDER BY symbol, exchange;
        """

        overviews: list[TickOverview] = []
        for row in self._iter_rows(sql):
            overview: TickOverview = TickOverview(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                count=int(row["count"]),
                start=self._from_questdb_datetime(row["start_datetime"]),
                end=self._from_questdb_datetime(row["end_datetime"]),
            )
            overviews.append(overview)

        return overviews

    def _iter_rows(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None
    ) -> list[DictRow]:
        """分批读取查询结果"""
        rows: list[DictRow] = []
        with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                while batch := cursor.fetchmany(FETCH_SIZE):
                    rows.extend(batch)
        return rows

    def _query_count(self, sql: str, params: tuple[Any, ...]) -> int:
        """查询单个count结果"""
        with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row: DictRow | None = cursor.fetchone()
                if not row:
                    return 0
                return int(row["count"])

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        """执行SQL语句"""
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    def _wait_wal_apply(self, table_name: str) -> None:
        """等待WAL事务应用到可查询表数据"""
        if self.wal_apply_timeout <= 0:
            return

        deadline: float = monotonic() + self.wal_apply_timeout
        sql: str = """
            SELECT
                suspended,
                writerTxn,
                sequencerTxn,
                errorMessage
            FROM wal_tables()
            WHERE name = %s;
        """

        while True:
            with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (table_name,))
                    row: DictRow | None = cursor.fetchone()

            if not row:
                return

            if row["suspended"]:
                raise RuntimeError(f"QuestDB WAL表{table_name}已暂停: {row['errorMessage']}")

            if row["writerTxn"] == row["sequencerTxn"]:
                return

            if monotonic() >= deadline:
                raise TimeoutError(f"等待QuestDB WAL表{table_name}应用超时")

            sleep(0.05)

    @staticmethod
    def _to_questdb_datetime(dt: datetime) -> datetime:
        """转换为QuestDB ILP写入使用的UTC时间"""
        db_dt: datetime = convert_tz(dt).replace(tzinfo=DB_TZ)
        return db_dt.astimezone(timezone.utc)

    @classmethod
    def _to_pg_datetime(cls, dt: datetime) -> datetime:
        """转换为PGWire查询使用的UTC naive时间"""
        return cls._to_questdb_datetime(dt).replace(tzinfo=None)

    @staticmethod
    def _from_questdb_datetime(dt: datetime) -> datetime:
        """将QuestDB返回的UTC naive时间转换为vn.py数据库时区"""
        if dt.tzinfo:
            return dt.astimezone(DB_TZ)

        return dt.replace(tzinfo=timezone.utc).astimezone(DB_TZ)
