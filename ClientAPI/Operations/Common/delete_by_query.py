import datetime
from typing import Union, Optional

from ravendb import DeleteByQueryOperation, AbstractIndexCreationTask, IndexQuery, QueryOperationOptions
from ravendb.documents.operations.definitions import IOperation, OperationIdResult

from examples_base import ExampleBase


# region the_index
# The index definition:
# =====================
class ProductsByPrice(AbstractIndexCreationTask):
    class IndexEntry:
        def __init__(self, price: int):
            self.price = price

    def __init__(self):
        super().__init__()
        self.map = "from product in products select new {price = product.PricePerUnit}"


# endregion


class DeleteByQuery(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_delete_by_query(self):
        with self.embedded_server.get_document_store() as store:
            # region delete_by_query_0
            # Define the delete by query operation, pass an RQL querying a collection
            delete_by_query_op = DeleteByQueryOperation("from 'Orders'")

            # Execute the operation by passing it to Operation.send_async
            operation = store.operations.send_async(delete_by_query_op)

            # All documents in collection 'Orders' will be deleted from the server
            # endregion

            # region delete_by_query_1
            # Define the delete by query operation, pass an RQL querying a collection
            delete_by_query_op = DeleteByQueryOperation("from 'Orders' where Freight > 30")

            # Execute the operation by passing it to Operation.send_async
            operation = store.operations.send_async(delete_by_query_op)

            # * All documents matching the specified RQL will be deleted from the server.
            #
            # * Since the dynamic query was made with a filtering condition,
            #   an auto-index is generated (if no other matching auto-index already exists).
            # endregion
            ProductsByPrice().execute(store)

            # region delete_by_query_2
            # Define the delete by query operation, pass an RQL querying the index
            delete_by_query_op = DeleteByQueryOperation("from index 'Products/ByPrice' where Price > 10")

            # Execute the operation by passing it to Operation.send_async
            operation = store.operations.send_async(delete_by_query_op)

            # All documents with document-field PricePerUnit > 10 will be deleted from the server.
            # endregion

            # region delete_by_query_3
            # Define the delete by query operation
            delete_by_query_op = DeleteByQueryOperation(
                IndexQuery(query="from index 'Products/ByPrice' where Price > 10")
            )

            # Execute the operation by passing it to Operation.send_async
            operation = store.operations.send_async(delete_by_query_op)

            # All documents with document-field PricePerUnit > 10 will be deleted from the server.
            # endregion
            # todo reeb: 4 & 5 not relevant to python
            # region delete_by_query_6
            # Define the delete by query operation
            delete_by_query_op = DeleteByQueryOperation(
                # QUERY: Specify the query
                IndexQuery(query="from index 'Products/ByPrice' where Price > 10"),
                # OPTIONS: Specify the options for the operations
                # (See all other available options in the Syntax section below)
                QueryOperationOptions(
                    # Allow the operation to operate even if index is stale
                    allow_stale=True,
                    # Get info in the operation result about documents that were deleted
                    retrieve_details=True,
                ),
            )

            # Execute the operation by passing it to Operations.send_async
            operation = store.operations.send_async(delete_by_query_op)

            # Wait for operation to complete
            result = operation.wait_for_completion(
                ...
            )  # todo reeb: wait for the merge of https://issues.hibernatingrhinos.com/issue/RDBC-841/Implement-Operation-wait-for-completion-with-timeout

            # * All documents with document-field PricePerUnit > 10 will be deleted from the server

            # * Details about deleted documents are available:
            details = result.details
            document_id_that_was_deleted = details[0]["Id"]
            # endregion

    class Foo:
        # region syntax_1
        class DeleteByQueryOperation(IOperation[OperationIdResult]):
            def __init__(
                self, query_to_delete: Union[str, IndexQuery], options: Optional[QueryOperationOptions] = None
            ): ...

        # endregion

        # todo reeb: it will change after RDBC-849
        # region syntax_2

        class QueryOperationOptions(object):
            def __init__(
                self,
                allow_stale: bool = True,
                stale_timeout: datetime.timedelta = None,
                max_ops_per_sec: int = None,
                retrieve_details: bool = False,
            ):
                # Indicates whether operations are allowed on stale indexes.
                # DEFAULT: True
                self.allow_stale = allow_stale

                # If allow_stale is set to false and index is stale,
                # then this is the maximum timeout to wait for index to become non-stale.
                # If timeout is exceeded then exception is thrown.
                # DEFAULT: None (if index is stale then exception is thrown immediately)
                self.stale_timeout = stale_timeout

                # Limits the number of base operations per second allowed.
                # DEFAULT: no limit
                self.max_ops_per_sec = max_ops_per_sec

                # Determines whether operation details about each document should be returned by server.
                # DEFAULT: False
                self.retrieve_details = retrieve_details

        # endregion
