from typing import TypeVar

from ravendb import DocumentQuery

from ClientAPI.Indexes.Querying.intersection import TShirts_ByManufacturerColorSizeAndReleaseYear, TShirtType, TShirt
from examples_base import ExampleBase

_T = TypeVar("_T")


class HowToUseIntersect(ExampleBase):
    def setUp(self):
        super().setUp()

    class Foo:
        # region intersect_1
        def intersect(self) -> DocumentQuery[_T]: ...

        # endregion

    def test_how_to_use_intersect(self):
        with self.embedded_server.get_document_store("UsingIntersect") as store:
            TShirts_ByManufacturerColorSizeAndReleaseYear().execute(store)
            with store.open_session() as session:
                session.store(TShirt("tshirts/1", 2000, "Stussy", [TShirtType("Gray", "Large")]))
                session.store(TShirt("tshirts/2", 2023, "Première", [TShirtType("Blue", "Small")]))
                session.store(TShirt("tshirts/3", 2018, "Supreme", [TShirtType("White", "Large")]))
                session.store(
                    TShirt("tshirts/4", 2024, "Raven", [TShirtType("Blue", "Small"), TShirtType("Gray", "Large")])
                )
                session.save_changes()
                # region intersect_2
                # return all T-shirts that are manufactured by 'Raven'
                # and contain both 'Small Blue' and 'Large Gray' types
                tshirts = list(
                    session.query_index_type(
                        TShirts_ByManufacturerColorSizeAndReleaseYear,
                        TShirts_ByManufacturerColorSizeAndReleaseYear.Result,
                    )
                    .where_equals("manufacturer", "Raven")
                    .intersect()
                    .where_equals("color", "Blue")
                    .and_also()
                    .where_equals("size", "Small")
                    .intersect()
                    .where_equals("color", "Gray")
                    .and_also()
                    .where_equals("size", "Large")
                    .of_type(TShirt)
                )
                # endregion
