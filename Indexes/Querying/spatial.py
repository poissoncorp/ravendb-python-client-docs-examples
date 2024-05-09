from ravendb import AbstractIndexCreationTask, PointField
from ravendb.documents.indexes.spatial.configuration import SpatialRelation

from examples_base import ExampleBase


class Event:
    def __init__(self, name: str = None, latitude: float = None, longitude: float = None):
        self.name = name
        self.latitude = latitude
        self.longitude = longitude


# region spatial_3_2
class Events_ByCoordinates(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from e in docs.Events select new { coordinates = CreateSpatialField(e.latitude, e.longitude) }"


# endregion


class Spatial(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_spatial(self):
        with self.embedded_server.get_document_store("SpatialIndexes") as store:
            with store.open_session() as session:
                # region spatial_1_0
                results = list(
                    session.query(object_type=Event).spatial(
                        PointField("latitude", "longitude"), lambda criteria: criteria.within_radius(500, 30, 30)
                    )
                )
                # endregion

                # region spatial_2_0
                results = list(
                    session.query(object_type=Event).spatial(
                        PointField("latitude", "longitude"),
                        lambda criteria: criteria.relates_to_shape("Circle(30 30 d=500.0000)", SpatialRelation.WITHIN),
                    )
                )
                # endregion

                # region spatial_3_0
                results = list(
                    session.query(object_type=Event).spatial(
                        "coordinates", lambda criteria: criteria.within_radius(500, 30, 30)
                    )
                )
                # endregion
