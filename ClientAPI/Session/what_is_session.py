from ravendb.infrastructure.orders import Company
from examples_base import ExamplesBase


class WhatIsSession(ExamplesBase):
    def test_samples(self):
        with self.embedded_server.get_document_store("WhatIsSession") as store:
            # A client-side copy of the document ID.
            company_id = None

            # region session_usage_1
            with store.open_session() as session:
                # Create a new entity
                entity = Company(name="CompanyName")

                # Store the entity in the Session's internal map
                session.store(entity)
                # From now on, any changes that will be made to the entity will be tracked by the Session.
                # However, the changes will be persisted to the server only when 'save_changes()' is called.

                session.save_changes()
                # At this point the entity is persisted to the database as a new document.
                # Since no database was specified when opening the Session, the Default Database is used.

            # endregion
