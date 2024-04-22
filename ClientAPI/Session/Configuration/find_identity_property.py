from typing import Callable

from examples_base import ExampleBase


class FindIdentityProperty(ExampleBase):
    def setUp(self):
        super().setUp()

    class Foo:
        # region identity_1
        find_identity_property: Callable = None
        # endregion

    def find_identity_property(self):
        with self.embedded_server.get_document_store("Find Identity Property") as store:
            with store.open_session() as session:
                # region identity_2
                # todo reeb & gracjan: skip this example, we have this but it doesn't work
                store.conventions._find_identity_property = lambda q: q.__name__ == "Identifier"
                # endregion
