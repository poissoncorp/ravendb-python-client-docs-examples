import datetime
from typing import Generic, TypeVar, Dict, Any

from ravendb import (
    MetadataAsDictionary,
    GetCompareExchangeValueOperation,
    GetCompareExchangeValuesOperation,
    PutCompareExchangeValueOperation,
    DeleteCompareExchangeValueOperation,
    SessionOptions,
    TransactionMode,
    IncludeBuilderBase,
)
from ravendb.primitives import constants
from ravendb.tools.utils import Utils

from ClientAPI.Session.Querying.DocumentQuery.query_vs_document_query import User
from examples_base import ExampleBase

_T = TypeVar("_T")


class Foo:
    # region compare_exchange_value
    class CompareExchangeValue(Generic[_T]):
        def __init__(self, key: str, index: int, value: _T, metadata: MetadataAsDictionary = None):
            self.key = key
            self.index = index
            self.value = value
            self._metadata_as_dictionary = metadata

    # endregion

    # region compare_exchange_result
    class CompareExchangeResult(Generic[_T]):
        def __init__(self, value: _T, index: int, successful: bool):
            self.value = value
            self.index = index
            self.successful = successful

    # endregion


class CompareExchangeExample(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_compare_exchange(self):
        with self.embedded_server.get_document_store("CompareExchange") as store:
            with store.open_session() as session:
                # region get_1
                read_result = store.operations.send(GetCompareExchangeValueOperation("NextClientId", int))
                value = read_result.value
                # endregion

            with store.open_session() as session:
                # region get_2
                read_result = store.operations.send(GetCompareExchangeValueOperation("AdminUser", User))

                admin = read_result.value
                # endregion

            with store.open_session() as session:
                # region get_list_2
                compare_exchange_values = store.operations.send(GetCompareExchangeValuesOperation(["Key-1", "Key-2"]))
                # endregion

            with store.open_session() as session:
                # region get_list_3
                # Get values for keys that have the common prefix 'users'
                # Retrieve maximum 20 entries
                compare_exchange_values = store.operations.send(
                    GetCompareExchangeValuesOperation.create_for_start_with("users", 0, 20)
                )
                # endregion

            with store.open_session() as session:
                # region put_1
                compare_exchange_result = store.operations.send(
                    PutCompareExchangeValueOperation("Emails/foo@example.org", "users/123", 0)
                )
                successful = compare_exchange_result.successful
                # If successful is true: then Key 'foo@example.org' now has the value of "users/123"
                # endregion

            with store.open_session() as session:
                # region put_2
                # Get existing value
                read_result = store.operations.send(GetCompareExchangeValueOperation("AdminUser", User))

                read_result.value.age += 1

                # Update value
                save_result = store.operations.send(
                    PutCompareExchangeValueOperation("AdminUser", read_result.value, read_result.index)
                )

                # The save result is successful only if 'index' wasn't changed between the read and write operations
                save_result_successful = save_result.successful
                # endregion

            with store.open_session() as session:
                # region delete_1
                # First, get existing value
                read_result = store.operations.send(GetCompareExchangeValueOperation("AdminUser", User))

                # Delete the key - use the index received from the 'Get' operation
                delete_result = store.operations.send(
                    DeleteCompareExchangeValueOperation("AdminUser", read_result.index)
                )

                # The delete result is successful only if the index has not changed between the read and delete operations
                delete_result_successful = delete_result.successful
                # endregion

            # region expiration_0
            with store.open_session() as session:
                # Set a time exactly one week from now
                expiration_time = datetime.datetime.now() + datetime.timedelta(days=7)

                # Create a new compare exchange value
                cmpxchg_value = session.advanced.cluster_transaction.create_compare_exchange_value("key", "value")

                # Edit metadata
                cmpxchg_value.metadata[constants.Documents.Metadata.EXPIRES] = Utils.datetime_to_string(expiration_time)

                # Send to server
                session.save_changes()
            # endregion

            with store.open_session() as session:
                # region expiration_1
                # Retrieve an existing key
                cmpxchg_value = store.operations.send(GetCompareExchangeValueOperation("key", str))

                # Set time
                expiration_time = datetime.datetime.utcnow() + datetime.timedelta(days=7)

                # Edit metadata
                cmpxchg_value.metadata[constants.Documents.Metadata.EXPIRES] = Utils.datetime_to_string(expiration_time)

                # Update value. Index must match the index on the server side,
                # or the operation will fail
                result = store.operations.send(
                    PutCompareExchangeValueOperation(cmpxchg_value.key, cmpxchg_value.value, cmpxchg_value.index)
                )
                # endregion

            # region metadata_0
            with store.open_session(
                session_options=SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
            ) as session:
                # Create a new compare exchange value
                cmpxchg_value = session.advanced.cluster_transaction.create_compare_exchange_value("key", "value")

                # Add a field to the metadata
                # with a value of type str
                cmpxchg_value.metadata["Field name"] = "some value"

                # Retrieve metadata as a dictionary
                cmpxchg_metadata = cmpxchg_value.metadata
            # endregion

            # region metadata_1
            cmpxchg_metadata = MetadataAsDictionary({"Year": "2020"})

            # Create/update the compare exchange value "best"
            store.operations.send(PutCompareExchangeValueOperation("best", User(name="Alice"), 0, cmpxchg_metadata))
            # endregion

            # -Include examples-

            # region include_load
            with store.open_session() as session:
                # Load a user document, include the compare exchange value
                # with the key equal to the user's Name field
                user = session.load("users/1-A", User, lambda includes: includes.include_compare_exchange_value("Name"))

                # Getting the compare exchange value does not require
                # another call to the server
                value = session.advanced.cluster_transaction.get_compare_exchange_value(user.name, str)
            # endregion
            # todo reeb: linq skipped on purpose
            with store.open_session() as session:
                # region include_raw_query
                # First method: from body of query
                users1 = list(session.advanced.raw_query("from Users as u select u include cmpxchg(u.Name)", User))

                compare_exchange_values_1 = []

                # Getting the compare exchange values does not require
                # additional calls to the server
                for u in users1:
                    compare_exchange_values_1.append(
                        session.advanced.cluster_transaction.get_compare_exchange_value(u.name)
                    )

                # Second method: from a JavaScript function
                users2 = list(
                    session.advanced.raw_query(
                        "declare function includeCEV(user) {"
                        "   includes.cmpxchg(user.Name)"
                        "   return user;"
                        "}"
                        ""
                        "from Users as u"
                        "select includeCEV(u)"
                    )
                )
                # Note that includeCEV() returns the same User
                # entity is received, without modyfing it

                compare_exchange_values_2 = []

                # Does not require calls to the server
                for u in users2:
                    compare_exchange_values_2.append(
                        session.advanced.cluster_transaction.get_compare_exchange_value(u.name)
                    )

                # endregion
            # todo reeb: we may include this definition below in docs (from_json and to_json ensure the valid casing on the server side)
            # region user
            class User:
                def __init__(self, name: str = None, age: int = None):
                    self.name = name
                    self.age = age

                @classmethod
                def from_json(cls, json_dict: Dict[str, Any]) -> "User":
                    return cls(json_dict["Name"], json_dict["Age"])

                def to_json(self) -> Dict[str, Any]:
                    return {"Name": self.name, "Age": self.age}

            # endregion

            # region include_builder
            def include_compare_exchange_value(self, path: str) -> IncludeBuilderBase: ...

            # endregion
