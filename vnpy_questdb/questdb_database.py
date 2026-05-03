from collections.abc import Iterator
from datetime import datetime, timezone
from time import monotonic, sleep
from typing import Any, TypeAlias

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
WAL_APPLY_TIMEOUT: float = 30

SqlValue: TypeAlias = str | int | float | bool | datetime | None
SqlParams: TypeAlias = tuple[SqlValue, ...]
IlpColumns: TypeAlias = dict[str, SqlValue]
RowTuple: TypeAlias = tuple[Any, ...]

CREATE_BAR_TABLE_SQL: str = f"""
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

CREATE_TICK_TABLE_SQL: str = f"""
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

LOAD_BAR_DATA_SQL: str = f"""
    SELECT
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
      AND deleted = false
    ORDER BY datetime;
"""

LOAD_TICK_DATA_SQL: str = f"""
    SELECT
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
      AND deleted = false
    ORDER BY datetime;
"""

COUNT_BAR_DATA_SQL: str = f"""
    SELECT count() AS count
    FROM {BAR_TABLE}
    WHERE symbol = %s
      AND exchange = %s
      AND interval = %s
      AND deleted = false;
"""

SOFT_DELETE_BAR_DATA_SQL: str = f"""
    UPDATE {BAR_TABLE}
    SET deleted = true
    WHERE symbol = %s
      AND exchange = %s
      AND interval = %s
      AND deleted = false;
"""

COUNT_TICK_DATA_SQL: str = f"""
    SELECT count() AS count
    FROM {TICK_TABLE}
    WHERE symbol = %s
      AND exchange = %s
      AND deleted = false;
"""

SOFT_DELETE_TICK_DATA_SQL: str = f"""
    UPDATE {TICK_TABLE}
    SET deleted = true
    WHERE symbol = %s
      AND exchange = %s
      AND deleted = false;
"""

GET_BAR_OVERVIEW_SQL: str = f"""
    SELECT
        symbol,
        exchange,
        interval,
        count() AS count,
        min(datetime) AS start_datetime,
        max(datetime) AS end_datetime
    FROM {BAR_TABLE}
    WHERE deleted = false
    GROUP BY symbol, exchange, interval
    ORDER BY symbol, exchange, interval;
"""

GET_TICK_OVERVIEW_SQL: str = f"""
    SELECT
        symbol,
        exchange,
        count() AS count,
        min(datetime) AS start_datetime,
        max(datetime) AS end_datetime
    FROM {TICK_TABLE}
    WHERE deleted = false
    GROUP BY symbol, exchange
    ORDER BY symbol, exchange;
"""

WAL_TABLE_STATUS_SQL: str = """
    SELECT
        suspended,
        writerTxn,
        sequencerTxn,
        errorMessage
    FROM wal_tables()
    WHERE name = %s;
"""


class QuestdbDatabase(BaseDatabase):
    """
    QuestDB数据库接口。
    """

    def __init__(self) -> None:
        """
        初始化QuestDB数据库接口。

        读取VeighNa数据库配置，构造PGWire连接参数和ILP写入配置，并确保
        K线和Tick数据表已经创建。
        """
        self.host: str = str(SETTINGS.get("database.host", "localhost"))
        self.port: int = int(SETTINGS.get("database.port", 8812))
        self.user: str = str(SETTINGS.get("database.user", "admin"))
        self.password: str = str(SETTINGS.get("database.password", "quest"))
        self.database: str = str(SETTINGS.get("database.database", "qdb"))
        self.http_port: int = int(SETTINGS.get("database.http_port", 9000))

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
        """
        创建QuestDB ILP客户端配置。

        Returns:
            QuestDB Sender使用的HTTP ILP连接配置字符串。
        """
        # ILP写入使用HTTP端口，PGWire查询使用独立的SQL端口。
        return f"http::addr={self.host}:{self.http_port};"

    def init_tables(self) -> None:
        """
        初始化数据库表。
        """
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(CREATE_BAR_TABLE_SQL)
                cursor.execute(CREATE_TICK_TABLE_SQL)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """
        保存K线数据。

        Args:
            bars: 待写入的K线数据列表。
            stream: VeighNa数据库接口兼容参数，QuestDB写入逻辑不区分该参数。

        Returns:
            写入成功返回True。

        Raises:
            ValueError: 当K线周期为空时抛出。
        """
        if not bars:
            return True

        with Sender.from_conf(self.ilp_conf) as sender:
            for bar in bars:
                interval: Interval | None = bar.interval
                if interval is None:
                    # interval是QuestDB去重主键的一部分，写入前必须明确。
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
        """
        保存Tick数据。

        Args:
            ticks: 待写入的Tick数据列表。
            stream: VeighNa数据库接口兼容参数，QuestDB写入逻辑不区分该参数。

        Returns:
            写入成功返回True。
        """
        if not ticks:
            return True

        with Sender.from_conf(self.ilp_conf) as sender:
            for tick in ticks:
                columns: IlpColumns = {
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
        """
        读取K线数据。

        Args:
            symbol: 合约代码。
            exchange: 交易所。
            interval: K线周期。
            start: 查询开始时间。
            end: 查询结束时间。

        Returns:
            按时间升序排列的K线数据列表。
        """
        params: SqlParams = (
            symbol,
            exchange.value,
            interval.value,
            self._to_pg_datetime(start),
            self._to_pg_datetime(end),
        )

        bars: list[BarData] = []
        append = bars.append
        from_datetime = self._from_questdb_datetime
        for row in self._iter_tuples(LOAD_BAR_DATA_SQL, params):
            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=from_datetime(row[0]),
                interval=interval,
                volume=row[1],
                turnover=row[2],
                open_interest=row[3],
                open_price=row[4],
                high_price=row[5],
                low_price=row[6],
                close_price=row[7],
                gateway_name="DB",
            )
            append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """
        读取Tick数据。

        Args:
            symbol: 合约代码。
            exchange: 交易所。
            start: 查询开始时间。
            end: 查询结束时间。

        Returns:
            按时间升序排列的Tick数据列表。
        """
        params: SqlParams = (
            symbol,
            exchange.value,
            self._to_pg_datetime(start),
            self._to_pg_datetime(end),
        )

        ticks: list[TickData] = []
        append = ticks.append
        from_datetime = self._from_questdb_datetime
        for row in self._iter_tuples(LOAD_TICK_DATA_SQL, params):
            localtime: datetime | None = None
            if row[33]:
                localtime = from_datetime(row[33])

            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=from_datetime(row[0]),
                name=row[1],
                volume=row[2],
                turnover=row[3],
                open_interest=row[4],
                last_price=row[5],
                last_volume=row[6],
                limit_up=row[7],
                limit_down=row[8],
                open_price=row[9],
                high_price=row[10],
                low_price=row[11],
                pre_close=row[12],
                bid_price_1=row[13],
                bid_price_2=row[14],
                bid_price_3=row[15],
                bid_price_4=row[16],
                bid_price_5=row[17],
                ask_price_1=row[18],
                ask_price_2=row[19],
                ask_price_3=row[20],
                ask_price_4=row[21],
                ask_price_5=row[22],
                bid_volume_1=row[23],
                bid_volume_2=row[24],
                bid_volume_3=row[25],
                bid_volume_4=row[26],
                bid_volume_5=row[27],
                ask_volume_1=row[28],
                ask_volume_2=row[29],
                ask_volume_3=row[30],
                ask_volume_4=row[31],
                ask_volume_5=row[32],
                localtime=localtime,
                gateway_name="DB",
            )
            append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """
        软删除K线数据。

        Args:
            symbol: 合约代码。
            exchange: 交易所。
            interval: K线周期。

        Returns:
            被标记删除的K线数据数量。
        """
        params: SqlParams = (symbol, exchange.value, interval.value)

        # 使用deleted标记软删除，避免直接移除QuestDB WAL表中的历史记录。
        count: int = self._query_count(COUNT_BAR_DATA_SQL, params)
        self._execute(SOFT_DELETE_BAR_DATA_SQL, params)
        self._wait_wal_apply(BAR_TABLE)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """
        软删除Tick数据。

        Args:
            symbol: 合约代码。
            exchange: 交易所。

        Returns:
            被标记删除的Tick数据数量。
        """
        params: SqlParams = (symbol, exchange.value)

        # 使用deleted标记软删除，避免直接移除QuestDB WAL表中的历史记录。
        count: int = self._query_count(COUNT_TICK_DATA_SQL, params)
        self._execute(SOFT_DELETE_TICK_DATA_SQL, params)
        self._wait_wal_apply(TICK_TABLE)

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """
        查询数据库中的K线汇总信息。

        Returns:
            K线汇总信息列表。
        """
        overviews: list[BarOverview] = []
        for row in self._iter_rows(GET_BAR_OVERVIEW_SQL):
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
        """
        查询数据库中的Tick汇总信息。

        Returns:
            Tick汇总信息列表。
        """
        overviews: list[TickOverview] = []
        for row in self._iter_rows(GET_TICK_OVERVIEW_SQL):
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
        params: SqlParams | None = None
    ) -> Iterator[DictRow]:
        """
        分批读取字典格式查询结果。

        Args:
            sql: 待执行的SQL语句。
            params: SQL查询参数。

        Yields:
            字典格式的查询结果行。
        """
        with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                while batch := cursor.fetchmany(FETCH_SIZE):
                    yield from batch

    def _iter_tuples(
        self,
        sql: str,
        params: SqlParams | None = None
    ) -> Iterator[RowTuple]:
        """
        分批读取元组格式查询结果。

        Args:
            sql: 待执行的SQL语句。
            params: SQL查询参数。

        Yields:
            元组格式的查询结果行。
        """
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                while batch := cursor.fetchmany(FETCH_SIZE):
                    yield from batch

    def _query_count(self, sql: str, params: SqlParams) -> int:
        """
        查询单个count结果。

        Args:
            sql: 返回count字段的SQL语句。
            params: SQL查询参数。

        Returns:
            查询到的count字段值。
        """
        with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row: DictRow | None = cursor.fetchone()
                if not row:
                    return 0
                return int(row["count"])

    def _execute(self, sql: str, params: SqlParams) -> None:
        """
        执行SQL语句。

        Args:
            sql: 待执行的SQL语句。
            params: SQL参数。
        """
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    def _wait_wal_apply(self, table_name: str) -> None:
        """
        等待WAL事务应用到可查询表数据。

        Args:
            table_name: QuestDB WAL表名。

        Raises:
            RuntimeError: 当WAL表处于暂停状态时抛出。
            TimeoutError: 当等待WAL事务应用超时时抛出。
        """
        if WAL_APPLY_TIMEOUT <= 0:
            return

        deadline: float = monotonic() + WAL_APPLY_TIMEOUT

        while True:
            with psycopg.connect(self.conninfo, row_factory=dict_row) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(WAL_TABLE_STATUS_SQL, (table_name,))
                    row: DictRow | None = cursor.fetchone()

            if not row:
                return

            if row["suspended"]:
                raise RuntimeError(f"QuestDB WAL表{table_name}已暂停: {row['errorMessage']}")

            # WAL写入需要等writer追上sequencer，后续PGWire查询才能稳定读到新数据。
            if row["writerTxn"] == row["sequencerTxn"]:
                return

            if monotonic() >= deadline:
                raise TimeoutError(f"等待QuestDB WAL表{table_name}应用超时")

            sleep(0.05)

    @staticmethod
    def _to_questdb_datetime(dt: datetime) -> datetime:
        """
        转换为QuestDB ILP写入使用的UTC时间。

        Args:
            dt: 待转换的时间。

        Returns:
            带UTC时区信息的时间。
        """
        db_dt: datetime = convert_tz(dt).replace(tzinfo=DB_TZ)
        return db_dt.astimezone(timezone.utc)

    @classmethod
    def _to_pg_datetime(cls, dt: datetime) -> datetime:
        """
        转换为PGWire查询使用的UTC naive时间。

        Args:
            dt: 待转换的时间。

        Returns:
            不带时区信息的UTC时间。
        """
        return cls._to_questdb_datetime(dt).replace(tzinfo=None)

    @staticmethod
    def _from_questdb_datetime(dt: datetime) -> datetime:
        """
        将QuestDB返回时间转换为VeighNa数据库时区。

        Args:
            dt: QuestDB返回的时间，可能带时区信息，也可能是UTC naive时间。

        Returns:
            转换到VeighNa数据库时区的时间。
        """
        if dt.tzinfo:
            return dt.astimezone(DB_TZ)

        return dt.replace(tzinfo=timezone.utc).astimezone(DB_TZ)
