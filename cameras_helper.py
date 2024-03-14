import datetime
import random
from typing import List


class Camera:
    def __init__(
        self,
        Id: str = None,
        date_of_listing: datetime.datetime = None,
        manufacturer: str = None,
        model: str = None,
        cost: float = None,
        zoom: int = None,
        megapixels: float = None,
        image_stabilizer: bool = None,
        advanced_features: List[str] = None,
    ):
        self.Id = Id
        self.date_of_listing = date_of_listing
        self.manufacturer = manufacturer
        self.model = model
        self.cost = cost
        self.zoom = zoom
        self.megapixels = megapixels
        self.image_stabilizer = image_stabilizer
        self.advanced_features = advanced_features


FEATURES = ["Image Stabilizer", "Tripod", "Low Light Compatible", "Fixed Lens", "LCD"]
MANUFACTURERS = ["Sony", "Nikon", "Phillips", "Canon", "Jessops"]
MODELS = ["Model1", "Model2", "Model3", "Model4", "Model5"]


def get_cameras(num_cameras: int) -> List[Camera]:
    camera_list = []
    for i in range(1, num_cameras):
        camera = Camera(
            date_of_listing=datetime.datetime(80 + random.randint(1, 30), random.randint(1, 12), random.randint(1, 27)),
            manufacturer=MANUFACTURERS[random.randint(0, len(MANUFACTURERS) - 1)],
            model=MODELS[random.randint(0, len(MODELS) - 1)],
            cost=random.randint(0, 100) * 9 + 100,
            zoom=random.randint(0, 10) + 1,
            megapixels=random.randint(0, 10) + 1,
            image_stabilizer=random.randint(0, 100) > 60,
            advanced_features=["??"],
        )
        camera_list.append(camera)
    return camera_list
