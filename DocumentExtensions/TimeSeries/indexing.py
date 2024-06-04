from datetime import datetime

from ravendb import PutIndexesOperation
from ravendb.documents.indexes.time_series import (
    TimeSeriesIndexDefinition,
    TimeSeriesIndexDefinitionBuilder,
    AbstractTimeSeriesIndexCreationTask,
    AbstractMultiMapTimeSeriesIndexCreationTask,
)

from examples_base import ExampleBase


class Indexing(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_indexing(self):
        with self.embedded_server.get_document_store("IndexingTimeSeriesData") as store:
            # region indexes_IndexDefinition
            ts_index_definition = TimeSeriesIndexDefinition(
                "Stocks_ByTradeVolume",
                maps={
                    "from ts in timeSeries.Companies.StockPrice "
                    "from entry in ts.Entries "
                    "select new { TradeVolume = entry.Values[4], entry.Timestamp.Date }"
                },
            )

            store.maintenance.send(PutIndexesOperation(ts_index_definition))
            # endregion

            # region indexes_IndexDefinitionBuilder
            ts_index_def_builder = TimeSeriesIndexDefinitionBuilder("Stocks_ByTradeVolume")
            ts_index_def_builder.map = (
                "from ts in timeSeries.Companies.StockPrice "
                "from entry in ts.Entries "
                "select new { TradeVolume = entry.Values[4], entry.Timestamp.Date }"
            )
            store.maintenance.send(PutIndexesOperation(ts_index_def_builder.to_index_definition(store.conventions)))
            # endregion

    # region indexes_CreationTask
    class Stocks_ByTradeVolume(AbstractTimeSeriesIndexCreationTask):
        def __init__(self):
            super().__init__()
            self.map = (
                "from ts in timeSeries.Companies.StockPrice "
                "from entry in ts.Entries "
                "select new { TradeVolume = entry.Values[4], entry.Timestamp.Date }"
            )

    # endregion

    # region indexes_MultiMapCreationTask
    class Vehicles_ByLocation(AbstractMultiMapTimeSeriesIndexCreationTask):
        def __init__(self):
            super().__init__()
            self._add_map(
                "from ts in timeSeries.Planes.GPS_Coordinates "
                "from entry in ts.Entries "
                "select new { Latitude = entry.Values[0], Longitude = entry.Values[1], entry.Timestamp }"
            )

            self._add_map(
                "from ts in timeSeries.Ships.GPS_Coordinates "
                "from entry in ts.Entries "
                "select new { Latitude = entry.Values[0], Longitude = entry.Values[1], entry.Timestamp"
            )

    # endregion

    # region indexes_MapReduce
    class TradeVolume_PerDay_ByCountry(AbstractTimeSeriesIndexCreationTask):
        class Result:
            def __init__(self, trade_volume: float = None, date: datetime = None, country: str = None):
                self.trade_volume = trade_volume
                self.date = date
                self.country = country

        def __init__(self):
            super().__init__()
            self.map = (
                "from ts in timeSeries.Companies.StockPrice "
                'let company = this.LoadDocument(ts.DocumentId, "Companies") '
                "from entry in ts.Entries "
                "select new "
                "{"
                "    trade_volume = entry.Values[4],"
                "    date = entry.Timestamp.Date,"
                "    country = entry.Address.Country "
                "}"
            )

            self.reduce = (
                "from r in results "
                "group r by new { r.Date, r.Country} into g "
                "select new "
                "{"
                "    trade_volume = g.Sum(x => x.trade_volume),"
                "    date = g.Key.Date,"
                "    country = g.Key.Country"
                "}"
            )

    # endregion
