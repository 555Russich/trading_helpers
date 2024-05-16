from dataclasses import dataclass
from datetime import datetime, date
from collections import UserList
from typing import Self, Literal

from trading_helpers.exceptions import IncorrectDatetimeConsistency

MathOperation = Literal['__add__', '__sub__', '__mul__', '__truediv__']


@dataclass(frozen=True)
class _Candle:
    open: float
    high: float
    low: float
    close: float
    volume: int
    time: datetime

    def multiple_by_constant(self, v: int | float) -> Self:
        return _Candle(
            open=self.open*v,
            high=self.high*v,
            low=self.low*v,
            close=self.close*v,
            volume=self.volume,
            time=self.time
        )

    def __add__(self, other: Self) -> Self:
        return _Candle(
            open=self.open + other.open,
            high=self.high + other.high,
            low=self.low + other.low,
            close=self.close + other.close,
            volume=self.volume + other.volume,
            time=self.time if self.time >= other.time else other.time
        )

    def __sub__(self, other: Self):
        return _Candle(
            open=self.open - other.open,
            high=self.high - other.high,
            low=self.low - other.low,
            close=self.close - other.close,
            volume=self.volume + other.volume,
            time=self.time if self.time >= other.time else other.time
        )

    def __mul__(self, other: Self):
        return _Candle(
            open=self.open * other.open,
            high=self.high * other.high,
            low=self.low * other.low,
            close=self.close * other.close,
            volume=self.volume + other.volume,
            time=self.time if self.time >= other.time else other.time
        )

    def __truediv__(self, other: Self):
        return _Candle(
            open=self.open / other.open,
            high=self.high / other.high,
            low=self.low / other.low,
            close=self.close / other.close,
            volume=self.volume + other.volume,
            time=self.time if self.time >= other.time else other.time
        )


class _Candles(UserList[_Candle]):
    HOLIDAYS: list[date]

    def check_datetime_consistency(self) -> None:
        for i in range(1, len(self)):
            if self[i-1].time > self[i].time:
                raise IncorrectDatetimeConsistency(f'Previous candle datetime value later than previous candle has: '
                                                   f'{self[i-1].time=} | {self[i].time=}')

    def remove_same_candles_in_a_row(self) -> Self:
        new_candles = _Candles()
        c1 = self[0]
        for i in range(1, len(self)):
            c2 = self[i]
            if not (c1.open == c2.open and c1.high == c2.high and c1.low == c2.low and
                    c1.close == c2.close and c1.volume == c2.volume and c1.time != c2.time):
                new_candles.append(c1)
                c1 = c2

        new_candles.append(self[-1])
        return new_candles

    def remove_weekend_and_holidays_candles(self) -> Self:
        candles = [c for c in self if c.time.weekday() not in (5, 6) and c.time.date() not in self.HOLIDAYS]
        return self.__class__(candles)

    def remove_weekend_candles(self) -> Self:
        return self.__class__([c for c in self if c.time.weekday() not in (5, 6)])

    def remove_holidays_candles(self) -> Self:
        return self.__class__([c for c in self if c.time.date() not in self.HOLIDAYS])

    def _do_math_operation(self, func_name: MathOperation, other: Self) -> Self:
        if len(self) == 0 or len(other) == 0:
            raise Exception(f'One of candles list is empty')

        candles = _Candles()
        i1, i2 = 0, 0

        while True:
            c1 = self[i1]
            c2 = other[i2]

            if c1.time == c2.time:
                i1 += 1
                i2 += 1
            elif c1.time > c2.time:
                c1 = self[i1 - 1]
                i2 += 1
            elif c1.time < c2.time:
                c2 = self[i2 - 1]
                i1 += 1

            c = getattr(c1, func_name)(c2)
            candles.append(c)

            if i1 == len(self) or i2 == len(other):
                assert i1 == len(self) and i2 == len(other), f'{i1=} | {len(self)=} ; {i2=} | {len(other)=}'
                return candles

    def __add__(self, other: Self) -> Self:
        return self._do_math_operation(func_name='__add__', other=other)

    def __sub__(self, other: Self) -> Self:
        return self._do_math_operation(func_name='__sub__', other=other)

    def __mul__(self, other: Self) -> Self:
        return self._do_math_operation(func_name='__mul__', other=other)

    def __truediv__(self, other: Self) -> Self:
        return self._do_math_operation(func_name='__truediv__', other=other)
