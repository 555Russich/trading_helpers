from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
from typing import TypeVar, Callable

import aiofiles

from trading_helpers.schemas import (
    CandleInterval,
    AnyCandle,
    AnyCandles,
)
from trading_helpers.exceptions import (
    CSVCandlesNeedAppend,
    CSVCandlesNeedInsert,
)


Interval = TypeVar('Interval')


class _CSVCandles(ABC):
    DELIMITER = ';'
    NEW_LINE = '\n'

    def __init__(self, instrument_id: str, interval: Interval):
        self.instrument_id = instrument_id
        self.interval = self.convert_candle_interval(interval)

    @classmethod
    @property
    @abstractmethod
    def CANDLE(cls) -> AnyCandle:
        raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def CANDLES(cls) -> AnyCandles:
        raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def COLUMNS(cls) -> dict[str, Callable]:
        raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def DIR_API(cls) -> Path:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def row2candle(cls, row: list[float | int | datetime]) -> AnyCandle:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def convert_candle_interval(cls, interval: Interval) -> CandleInterval:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def download_or_read(cls, *args, **kwargs) -> AnyCandles:
        raise NotImplementedError

    @property
    def filepath(self) -> Path:
        return (self.DIR_API / self.interval / self.instrument_id).with_suffix('.csv')

    async def _read(self, from_: datetime, to: datetime, interval: CandleInterval) -> AnyCandles:
        candles = self.CANDLES()

        async with aiofiles.open(self.filepath, 'r') as f:
            data = await f.readlines()

        for i, row in enumerate(data[1:], start=1):
            str_values = row.replace(self.NEW_LINE, '').split(self.DELIMITER)
            values = []

            for column, value in zip(self.COLUMNS.keys(), str_values):
                func_convert = self.COLUMNS[column]
                values.append(func_convert(value))

            candle = self.row2candle(values)
            if from_ <= candle.dt <= to:
                candles.append(candle)

            if i == 0 and candle.dt > from_:
                if not (interval == CandleInterval.DAY and candle.dt.date() == from_.date()):
                    raise CSVCandlesNeedInsert(to_temp=candle.dt)
            if i == len(data) - 1 and candle.dt < to:
                dt_delta = to - candle.dt
                if (
                        (
                                candle.dt.date() == to.date() and
                                interval == CandleInterval.MIN_1 and dt_delta > timedelta(minutes=1+1) or
                                interval == CandleInterval.MIN_5 and dt_delta > timedelta(minutes=5+1) or
                                interval == CandleInterval.HOUR and dt_delta > timedelta(minutes=60+1)
                        ) or
                        (candle.dt.date() < to.date() and interval == CandleInterval.DAY)
                ):
                    raise CSVCandlesNeedAppend(from_temp=candle.dt, candles=candles)
        return candles

    async def _append(self, candles: AnyCandles) -> None:
        if not candles:
            return

        data = self.NEW_LINE.join(
            self.DELIMITER.join(
                str(v) for v in (candle.open, candle.high, candle.low, candle.close, candle.volume, candle.dt)
            ) for candle in candles
        ) + self.NEW_LINE

        async with aiofiles.open(self.filepath, 'a') as f:
            await f.write(data)

    async def _insert(self, candles: AnyCandles):
        async with aiofiles.open(self.filepath, 'r') as f:
            data = await f.readlines()

        await self._prepare_new()
        await self._append(candles)
        async with aiofiles.open(self.filepath, 'a') as f:
            await f.writelines(data[1:])

    async def _prepare_new(self):
        async with aiofiles.open(self.filepath, 'w') as f:
            await f.write(self.DELIMITER.join(self.COLUMNS) + self.NEW_LINE)
