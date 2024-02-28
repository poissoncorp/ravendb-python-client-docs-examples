from typing import Optional

from ravendb import (
    CreateDatabaseOperation,
)
from ravendb import DocumentStore as DocumentStoreReal
from ravendb.infrastructure.orders import Employee
from ravendb.serverwide.database_record import DatabaseRecord

from examples_base import ExamplesBase


class DocumentStoreFake(DocumentStoreReal):
    def __init__(self):
        super().__init__("http://127.0.0.1:8080", "OpeningSession")
        self.initialize()
        try:
            self.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord("OpeningSession")))
        except RuntimeError as e:
            if "ConcurrencyException" in e.args[0]:
                pass

        with self.open_session() as session:
            session.store(Employee(first_name="MC Ride"))
            session.save_changes()


class DocumentStoreFakeUninitialized(DocumentStoreReal):
    def __init__(self):
        super().__init__("http://127.0.0.1:8080", "OpeningSession")


class OpeningSession(ExamplesBase):
    def setUp(self):
        super().setUp()

    class IFoo:
        #region store_entities_1
        def store(self, entity: object, key: Optional[str] = None, change_vector: Optional[str] = None) -> None:
            ...
        #endregion

    def test_storing_entities(self):
        with self.embedded_server.get_document_store("StoringEntities") as store:
            with store.open_session() as session:
                #region store_entities_5
                session.store(Employee(first_name="John", last_name="Doe"))

                # send all pending operations to server, in this case only 'Put' operation
                session.save_changes()
                #endregion




