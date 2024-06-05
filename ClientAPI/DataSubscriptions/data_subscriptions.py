import logging
from concurrent.futures import Future
from datetime import timedelta
from typing import Optional, Type, TypeVar, Callable, Any

from ravendb import DocumentStore
from ravendb.documents.session.loaders.include import SubscriptionIncludeBuilder
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionUpdateOptions,
    SubscriptionWorkerOptions,
    SubscriptionOpeningStrategy,
)
from ravendb.documents.subscriptions.state import SubscriptionState
from ravendb.documents.subscriptions.worker import SubscriptionWorker, SubscriptionBatch
from ravendb.exceptions.exceptions import (
    DatabaseDoesNotExistException,
    SubscriptionDoesNotExistException,
    SubscriptionInvalidStateException,
    AuthorizationException,
    SubscriptionClosedException,
    SubscriberErrorException,
    SubscriptionInUseException,
)

from examples_base import Order, ExampleBase

_T = TypeVar("_T")


class OrderRevenues:
    def __init__(self, previous_revenue: int = None, current_revenue: int = None):
        self.previous_revenue = previous_revenue
        self.current_revenue = current_revenue


class Foo:
    # region subscriptionCreationOverloads
    def create_for_options(self, options: SubscriptionCreationOptions, database: Optional[str] = None) -> str: ...

    def create_for_class(
        self,
        object_type: Type[_T],
        options: Optional[SubscriptionCreationOptions] = None,
        database: Optional[str] = None,
    ) -> str: ...

    # endregion
    # region updating_subscription
    def update(self, options: SubscriptionUpdateOptions, database: Optional[str] = None) -> str: ...

    # endregion

    # region sub_create_options
    class SubscriptionCreationOptions:
        def __init__(
            self,
            name: Optional[str] = None,
            query: Optional[str] = None,
            includes: Optional[Callable[[SubscriptionIncludeBuilder], None]] = None,
            change_vector: Optional[str] = None,
            mentor_node: Optional[str] = None,
        ):
            self.name = name
            self.query = query
            self.includes = includes
            self.change_vector = change_vector
            self.mentor_node = mentor_node

    # endregion

    # region sub_update_options

    class SubscriptionUpdateOptions(SubscriptionCreationOptions):
        def __init__(
            self,
            name: Optional[str] = None,
            query: Optional[str] = None,
            includes: Optional[Callable[[SubscriptionIncludeBuilder], None]] = None,
            change_vector: Optional[str] = None,
            mentor_node: Optional[str] = None,
            key: Optional[int] = None,
            create_new: Optional[bool] = None,
        ): ...

    # endregion
    # region subscriptionWorkerGeneration
    def get_subscription_worker(
        self, options: SubscriptionWorkerOptions, object_type: Optional[Type[_T]] = None, database: Optional[str] = None
    ) -> SubscriptionWorker[_T]: ...

    def get_subscription_worker_by_name(
        self,
        subscription_name: Optional[str] = None,
        object_type: Optional[Type[_T]] = None,
        database: Optional[str] = None,
    ) -> SubscriptionWorker[_T]: ...

    # endregion

    # region subscriptionWorkerRunning
    def run(self, process_documents: Optional[Callable[[SubscriptionBatch[_T]], Any]]) -> Future[None]: ...

    # endregion

    # region subscriptions_example
    def worker(self, store: DocumentStore) -> Future[None]:
        subscription_name = store.subscriptions.create_for_class(
            Order, SubscriptionCreationOptions(query="from Orders where Company = " "")
        )
        subscription = store.subscriptions.get_subscription_worker_by_name(subscription_name)

        def __subscription_callback(subscription_batch: SubscriptionBatch[Order]):
            for item in subscription_batch.items:
                print(f"Order #{item.result.key}")

        subscription_task = subscription.run(__subscription_callback)

        return subscription_task

    # endregion

    # region creation_api
    # endregion


class SubscriptionExamples(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_subscriptions(self):
        with self.embedded_server.get_document_store("SubscriptionsExamples") as store:
            # region create_whole_collection_generic_with_name
            name = store.subscriptions.create_for_class(
                Order, SubscriptionCreationOptions(name="OrdersProcessingSubscription")
            )
            # endregion

            # region create_whole_collection_generic_with_mentor_node
            name = store.subscriptions.create_for_class(Order, SubscriptionCreationOptions(mentor_node="D"))
            # endregion

            # region create_whole_collection_generic1
            name = store.subscriptions.create_for_class(Order)
            # endregion

            # region create_whole_collection_RQL
            name = store.subscriptions.create_for_options(SubscriptionCreationOptions(query="From Orders"))
            # endregion

            # region create_filter_only_RQL
            name = store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query=(
                        "declare function getOrderLinesSum(doc){"
                        "    var sum = 0;"
                        "    for (var i in doc.Lines) { sum += doc.Lines[i]; }"
                        "    return sum;"
                        "}"
                        "From Orders as o"
                        "Where getOrderLinesSum(o) > 100"
                    )
                ),
            )
            # endregion

            # region create_filter_and_projection_RQL
            name = store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query="""
                    declare function getOrderLinesSum(doc){
                        var sum = 0;
                        for (var i in doc.Lines) { sum += doc.Lines[i]; }
                        return sum;
                    }
                    
                    declare function projectOrder(doc){
                        return {
                            Id: order.Id,
                            Total: getOrderLinesSum(order)
                        };
                    }
                    
                    From Orders as o
                    Where getOrderLinesSum(o) > 100
                    Select projectOrder(o)
                    """
                )
            )
            # endregion

            # region create_filter_and_load_document_RQL
            name = store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query="""
                    declare function getOrderLinesSum(doc){
                        var sum = 0;
                        for (var i in doc.Lines) { sum += doc.Lines[i]; }
                        return sum;
                    }
                   
                    declare function projectOrder(doc){
                        return {
                            Id: order.Id,
                            Total: getOrderLinesSum(order)
                        };
                    }
                   
                    From Orders as o
                    Where getOrderLinesSum(o)
                    """
                )
            )
            # endregion

            # region create_filter_and_load_document_RQL
            name = store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query="""
                    declare function getOrderLinesSum(doc){
                        var sum = 0;
                        for (var i in doc.Lines) { sum += doc.Lines[i];}
                        return sum;
                    }

                    declare function projectOrder(doc){
                        var employee = load(doc.Employee);
                        return {
                            Id: order.Id,
                            Total: getOrderLinesSum(order),
                            ShipTo: order.ShipTo,
                            EmployeeName: employee.FirstName + ' ' + employee.LastName

                        };
                    }

                    From Orders as o 
                    Where getOrderLinesSum(o) > 100
                    Select projectOrder(o)"
                    """
                )
            )
            # endregion

            # region create_simple_revisions_subscription_generic
            # todo reeb: skip revisions + generic (No-RQL) subscriptions parts - not implemented yet
            # endregion

            # region use_simple_revisions_subscription_generic
            # todo reeb: skip revisions + generic (No-RQL) subscriptions parts - not implemented yet
            # endregion

            # region create_projected_revisions_subscription_generic
            # todo reeb: skip revisions + generic (No-RQL) subscriptions parts - not implemented yet
            # endregion

            # region create_projected_revisions_subscription_RQL
            name = store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query="""
                    declare function getOrderLinesSum(doc){
                        var sum = 0;
                        for (var i in doc.Lines) { sum += doc.Lines[i];}
                        return sum;
                    }

                    From Orders (Revisions = true)
                    Where getOrderLinesSum(this.Current)  > getOrderLinesSum(this.Previous)
                    Select 
                    {
                        previous_revenue: getOrderLinesSum(this.Previous),
                        current_revenue: getOrderLinesSum(this.Current)                            
                    }
                    """
                )
            )
            # endregion

            # region consume_revisions_subscription_generic
            revenues_comparison_worker = store.subscriptions.get_subscription_worker_by_name(name, OrderRevenues)

            def _revenues_callback(batch: SubscriptionBatch[OrderRevenues]):
                for item in batch.items:
                    print(
                        f"Revenue for order with Id: {item.key} grown from {item.result.previous_revenue} to {item.result.current_revenue}"
                    )

            revenues_comparison_worker.run(_revenues_callback)
            # endregion

            # region consumption_0
            subscription_name = store.subscriptions.create_for_class(
                Order, SubscriptionCreationOptions(query='From Orders Where Company = "companies/11"')
            )

            subscription = store.subscriptions.get_subscription_worker_by_name(subscription_name, Order)

            def _orders_callback(batch: SubscriptionBatch[Order]):
                for item in batch.items:
                    print(f"Order {item.result.key} will be shipped via: {item.result.ship_via}")

            subscription_task = subscription.run(_orders_callback)

            subscription_task.result()  # Optionally, set a timeout, or wrap it in an asyncio.Future
            # endregion
            # region open_1
            subscription = store.subscriptions.get_subscription_worker_by_name(name, Order)
            # endregion

            # region open_2
            subscription = store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(name, strategy=SubscriptionOpeningStrategy.WAIT_FOR_FREE)
            )
            # endregion

            # region open_3
            subscription = store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(
                    name,
                    strategy=SubscriptionOpeningStrategy.WAIT_FOR_FREE,
                    max_docs_per_batch=500,
                    ignore_subscriber_errors=True,
                )
            )
            # endregion

            # region create_subscription_with_includes_strongly_typed
            store.subscriptions.create_for_class(
                Order,
                SubscriptionCreationOptions(includes=lambda builder: builder.include_documents("Lines[].Product")),
            )
            # endregion

            # region create_subscription_with_includes_rql_path
            store.subscriptions.create_for_options(
                SubscriptionCreationOptions(query="from Orders include Lines[].Product")
            )
            # endregion

            # region create_subscription_with_includes_rql_javascript
            store.subscriptions.create_for_options(
                SubscriptionCreationOptions(
                    query="""
                declare function includeProducts(doc) 
                {
                    doc.IncludedFields=0;
                    doc.LinesCount = doc.Lines.length;
                    for (let i=0; i< doc.Lines.length; i++)
                    {
                        doc.IncludedFields++;
                        include(doc.Lines[i].Product);
                    }
                    return doc;
                }
                from Orders as o select includeProducts(o)
                """
                )
            )

            # endregion

            # region include_builder_counter_methods
            def include_counter(self, name: str) -> SubscriptionIncludeBuilder: ...

            def include_counters(self, *names: str) -> SubscriptionIncludeBuilder: ...

            def include_all_counters(self) -> SubscriptionIncludeBuilder: ...

            # endregion

            # region create_subscription_include_counters_builder
            store.subscriptions.create_for_class(
                Order,
                SubscriptionCreationOptions(
                    includes=lambda builder: builder.include_counter("numLines")
                    .include_counters(["pros", "cons"])
                    .include_all_counters()
                ),
            )
            # endregion

            # region update_subscription_example_0
            store.subscriptions.update(SubscriptionUpdateOptions(name="My subscription", query="From Orders"))
            # endregion

            # region update_subscription_example_1
            my_subscription = store.subscriptions.get_subscription_state("my subscription")

            subscription_id = my_subscription.subscription_id

            store.subscriptions.update(SubscriptionUpdateOptions(key=subscription_id, name="new name"))

            # endregion

            # region interface_subscription_deletion
            def delete(self, name: str, database: Optional[str] = None) -> None: ...

            # endregion

            # region interface_connection_dropping
            def drop_connection(self, name: str, database: Optional[str] = None) -> None: ...

            # endregion

            # region interface_subscription_enabling
            def enable(self, name: str, database: Optional[str] = None) -> None: ...

            # endregion

            # region interface_subscription_disabling
            def disable(self, name: str, database: Optional[str] = None) -> None: ...

            # endregion

            # region interface_subscription_state
            def get_subscription_state(
                self, subscription_name: str, database: Optional[str] = None
            ) -> SubscriptionState: ...

            # endregion

            subscription_name = ""

            # region subscription_enabling
            store.subscriptions.enable(subscription_name)
            # endregion

            # region subscription_disabling
            store.subscriptions.disable(subscription_name)
            # endregion
            # region subscription_deletion
            store.subscriptions.delete(subscription_name)
            # endregion

            # region connection_dropping
            store.subscriptions.drop_connection(subscription_name)
            # endregion

            # region subscription_state
            store.subscriptions.get_subscription_state(subscription_name)
            # endregion

            # region subscription_open_simple
            subscription_worker = store.subscriptions.get_subscription_worker_by_name(subscription_name, Order)
            # endregion

            # region subscription_run_simple
            subscription_runtime_task = subscription_worker.run(
                process_documents=lambda batch: ...
            )  # Pass your method that takes SubscriptionBatch[_T] as an argument, with your logic in it
            # endregion

            # region subscription_worker_with_batch_size
            worker_w_batch = store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(subscription_name, max_docs_per_batch=20), Order
            )

            _ = worker_w_batch.run(
                process_documents=lambda batch: ...
            )  # Pass your method that takes SubscriptionBatch[_T] as an argument, with your logic in it
            # endregion

            # region throw_during_user_logic
            def _throw_exception(batch: SubscriptionBatch):
                raise Exception()

            _ = worker_w_batch.run(_throw_exception)
            # endregion
            logger = logging.Logger("my_logger")

            class UnsupportedCompanyException(Exception): ...

            process_order = lambda x: ...
            # region reconnecting_client
            while True:
                options = SubscriptionWorkerOptions(subscription_name)

                # here we configure that we allow a down time of up to 2 hours, and will wait for 2 minutes for reconnecting
                options.max_erroneous_period = timedelta(hours=2)
                options.time_to_wait_before_connection_retry = timedelta(minutes=2)

                subscription_worker = store.subscriptions.get_subscription_worker(options, Order)

                try:
                    # here we are able to be informed of any exceptions that happens during processing
                    subscription_worker.add_on_subscription_connection_retry(
                        lambda exception: logger.error(
                            f"Error during subscription processing: {subscription_name}", exc_info=exception
                        )
                    )

                    def _process_documents_callback(batch: SubscriptionBatch[Order]):
                        for item in batch.items:
                            # we want to force close the subscription processing in that case
                            # and let the external code decide what to do with that
                            if item.result.company == "companies/832-A":
                                raise UnsupportedCompanyException(
                                    "Company Id can't be 'companies/832-A', you must fix this"
                                )
                            process_order(item.result)

                        # Run will complete normally if you have disposed the subscription
                        return

                    # Pass the callback to worker.run()
                    subscription_worker.run(_process_documents_callback)

                except Exception as e:
                    logger.error(f"Failure in subscription: {subscription_name}", exc_info=e)
                    exception_type = type(e)
                    if (
                        exception_type is DatabaseDoesNotExistException
                        or exception_type is SubscriptionDoesNotExistException
                        or exception_type is SubscriptionInvalidStateException
                        or exception_type is AuthorizationException
                    ):
                        raise  # not recoverable

                    if exception_type is SubscriptionClosedException:
                        # closed explicitely by admin, probably
                        return

                    if exception_type is SubscriberErrorException:
                        # for UnsupportedCompanyException type, we want to throw an exception, otherwise
                        # we continue processing
                        if e.args[1] is not None and type(e.args[1]) is UnsupportedCompanyException:
                            raise

                        continue

                    # handle this depending on subscription
                    # open strategy (discussed later)
                    if e is SubscriptionInUseException:
                        continue

                    return
                finally:
                    subscription_worker.__exit__()
            # endregion
