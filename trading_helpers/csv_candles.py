from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
from typing import TypeVar, Callable, ClassVar

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

    CANDLE = ClassVar[AnyCandle]
    CANDLES = ClassVar[AnyCandles]
    COLUMNS: dict[str, Callable]
    DIR_API: Path

    def __init__(self, instrument_id: str, interval: Interval | CandleInterval):
        self.instrument_id = instrument_id

        if not isinstance(interval, CandleInterval):
            self.interval = self.convert_candle_interval(interval)
        else:
            self.interval = interval

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
            candle_dict = {c: self.COLUMNS[c](v) for c, v in zip(self.COLUMNS, str_values)}
            candle = self.CANDLE(**candle_dict)

            if from_ <= candle.dt <= to:
                candles.append(candle)

            if i == 0 and candle.dt > from_:
                if not (interval == CandleInterval.DAY and candle.dt.date() == from_.date()):
                    raise CSVCandlesNeedInsert(to_temp=candle.dt)
            if i == len(data) - 1 and candle.dt < to:
                dt_delta = to - candle.dt
                if ((
                        candle.dt.date() == to.date() and
                        interval == CandleInterval.MIN_1 and dt_delta > timedelta(minutes=1+1) or
                        interval == CandleInterval.MIN_5 and dt_delta > timedelta(minutes=5+1) or
                        interval == CandleInterval.HOUR and dt_delta > timedelta(minutes=60+1)
                ) or (candle.dt.date() < to.date() and interval == CandleInterval.DAY)):
                    raise CSVCandlesNeedAppend(from_temp=candle.dt, candles=candles)
        return candles

    async def _append(self, candles: AnyCandles) -> None:
        if not candles:
            return

        data = self.NEW_LINE.join(
            self.DELIMITER.join(str(c.__dict__[k]) for k in self.COLUMNS) for c in candles
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
