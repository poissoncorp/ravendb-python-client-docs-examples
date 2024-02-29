from typing import Type, Optional, TypeVar

from ravendb import SessionOptions, TransactionMode
from ravendb.infrastructure.orders import Employee, Product
from examples_base import ExamplesBase

_T = TypeVar("_T")
_TIndex = TypeVar("_TIndex")


class HowToQuery(ExamplesBase):
    def setUp(self):
        super().setUp()

    def test_sync(self):
        with self.embedded_server.get_document_store("ClusterTransactionOverviewSync") as store:
            # region open_cluster_session_sync
            with store.open_session(
                session_options=SessionOptions(
                    transaction_mode=TransactionMode.CLUSTER_WIDE  # Set mode to be cluster-wide
                    # Session will be single-node when either:
                    #   * Mode is not specified
                    #   * Explicitly set TransactionMode.SINGLE_NODE
                )
            ) as session:
            # endregion
                ...
