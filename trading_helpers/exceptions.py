from datetime import datetime

from trading_helpers.schemas import AnyCandles


class IncorrectDatetimeConsistency(Exception):
    pass


class IncorrectFirstCandle(Exception):
    pass


class UnexpectedCandleInterval(Exception):
    pass


class CSVCandlesError(Exception):
    pass


class CSVCandlesNeedInsert(CSVCandlesError):
    def __init__(self, to_temp: datetime):
        self.to_temp = to_temp


class CSVCandlesNeedAppend(CSVCandlesError):
    def __init__(self, from_temp: datetime, candles: AnyCandles):
        self.from_temp = from_temp
        self.candles = candles
