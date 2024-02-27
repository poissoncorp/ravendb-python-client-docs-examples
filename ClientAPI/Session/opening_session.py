from ravendb import DocumentStore, RequestExecutor, SessionOptions
from ravendb.documents.conventions import DocumentConventions

from examples_base import ExamplesBase


class WhatIsSession(ExamplesBase):
    def setUp(self):
        super().setUp()
    def test_open_session(self):
        with self.embedded_server.get_document_store("YourDatabase") as store:
            # region open_session_1
            # Open session for a 'default' database configured in 'DocumentStore'
            with store.open_session() as session:
                doc = session.load("doc/1")
                # code here

            # Open session for a specific database
            with store.open_session(database="YourDatabase") as session:
                doc = session.load("doc/2")
                # code here

            # endregion
