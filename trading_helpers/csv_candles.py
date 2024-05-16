from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
from typing import ClassVar, TypeVar
import logging

import aiofiles

from trading_helpers.schemas import (
    CandleInterval,
    AnyCandle,
    AnyCandles,
)
from trading_helpers.exceptions import (
    IncorrectFirstCandle,
    CSVCandlesNeedAppend,
    CSVCandlesNeedInsert,
)


Interval = TypeVar('Interval')


class _CSVCandles(ABC):
    DELIMITER = ';'
    NEW_LINE = '\n'
    CANDLE: ClassVar[AnyCandle]
    CANDLES: ClassVar[AnyCandles]
    COLUMNS: ClassVar[tuple]

    def __init__(self, filepath: Path, interval: Interval):
        self.filepath = filepath
        self.interval = interval

    @classmethod
    @abstractmethod
    def get_filepath(cls, *args, **kwargs) -> Path:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def row2candle(cls, row: list[float | int | datetime]) -> AnyCandle:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def configure_datetime_from(cls, from_: datetime, **kwargs) -> datetime:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def convert_candle_interval(cls, interval: Interval) -> CandleInterval:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def download_or_read(cls) -> AnyCandles:
        raise NotImplementedError

    # @classmethod
    # async def download_or_read(
    #         cls,
    #         instrument_id: str,
    #         from_: datetime,
    #         to: datetime,
    #         interval: Interval,
    # ) -> AnyCandles:
    #     candles = None
    #     from_ = cls.configure_datetime_from(from_=from_, instrument=instrument)
    #
    #     filepath = cls.get_filepath(instrument, interval=interval)
    #     csv = cls(filepath, interval=interval)
    #
    #     if not filepath.exists():
    #         logging.debug(f'File not exists | {instrument_id=}')
    #         await csv._prepare_new()
    #         candles = await get_candles(instrument_id=instrument.uid, from_=from_, to=to, interval=interval)
    #         await csv._append(candles)
    #         return candles
    #
    #     for retry in range(1, 4):
    #         try:
    #             return await csv._read(from_=from_, to=to, interval=interval)
    #         except CSVCandlesNeedAppend as ex:
    #             logging.debug(f'Need append | {retry=} | ticker={instrument.ticker} | uid={instrument.uid} | from_temp='
    #                           f'{dt_form_sys.datetime_strf(ex.from_temp)} | to={dt_form_sys.datetime_strf(to)}')
    #             # 1st candle in response is last candle in file
    #             candles = (await get_candles(instrument_id=instrument.uid, from_=ex.from_temp, to=to, interval=interval))[1:]
    #
    #             if not candles or (len(candles) == 1 and candles[0].is_complete is False):
    #                 to = ex.candles[-1].dt if to > ex.candles[-1].dt else to
    #
    #             if candles:
    #                 candles = candles if candles[-1].is_complete else candles[:-1]
    #                 await csv._append(candles)
    #         except CSVCandlesNeedInsert as ex:
    #             logging.debug(f'Need insert | {retry=} | ticker={instrument.ticker} | uid={instrument.uid} |'
    #                           f' from={dt_form_sys.datetime_strf(from_)} | '
    #                           f'to_temp={dt_form_sys.datetime_strf(ex.to_temp)}')
    #             if retry == 3:
    #                 raise IncorrectFirstCandle(f'{candles[0].dt=} | {from_=}')
    #
    #             candles = await get_candles(instrument_id=instrument.uid, from_=from_, to=ex.to_temp, interval=interval)
    #             # 1st candle in file is last candle in get_candles response
    #             candles = candles[:-1]
    #
    #             if candles:
    #                 await csv._insert(candles[:-1])
    #             else:
    #                 logging.debug(f'Nothing between from_={dt_form_sys.datetime_strf(from_)} and to_temp='
    #                               f'{dt_form_sys.datetime_strf(ex.to_temp)}')
    #                 from_ = ex.to_temp
    #         except Exception as ex:
    #             logging.error(f'{retry=} | {csv.filepath} | {instrument.ticker=}\n{ex}', exc_info=True)
    #             raise ex

    async def _read(self, from_: datetime, to: datetime, interval: CandleInterval) -> AnyCandles:
        candles = self.CANDLES()

        async with aiofiles.open(self.filepath, 'r') as f:
            data = await f.readlines()

        columns = data[0].replace(self.NEW_LINE, '').split(self.DELIMITER)
        data = data[1:]

        for i, row in enumerate(data):
            str_values = row.replace(self.NEW_LINE, '').split(self.DELIMITER)
            values = []

            for column, value in zip(columns, str_values):
                if column in ('open', 'high', 'low', 'close'):
                    values.append(float(value))
                elif column == 'volume':
                    values.append(int(value))
                elif column == 'time':
                    values.append(datetime.fromisoformat(value))

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
