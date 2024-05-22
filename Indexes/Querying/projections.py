from ravendb import AbstractIndexCreationTask, QueryData
from ravendb.documents.indexes.definitions import FieldStorage

from examples_base import ExampleBase, Employee, Order, Company


# region indexes_1
class Employees_ByFirstAndLastName(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from employee in docs.Employees "
            "select new "
            "{"
            " FirstName = employee.FirstName,"
            " LastName = employee.LastName"
            "}"
        )


# endregion


# region indexes_1_stored
class Employees_ByFirstAndLastNameWithStoredFields(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from employee in docs.Employees "
            "select new"
            "{"
            " FirstName = employee.FirstName,"
            " LastName = employee.LastName"
            "}"
        )
        self._store_all_fields(FieldStorage.YES)


# endregion


# region indexes_2
class Employees_ByFirstNameAndBirthday(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from employee in docs.Employees "
            "select new "
            "{"
            " FirstName = employee.FirstName,"
            " Birthday = employee.Birthday"
            "}"
        )


# endregion


# region indexes_3
class Orders_ByShipToAndLines(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from order in docs.Orders select new { ShipTo = order.ShipTo, Lines = order.Lines}"


# endregion
# region indexes_4
class Orders_ByShippedAtAndCompany(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from order in docs.Orders "
            "select new "
            "{"
            " ShippedAt = order.ShippedAt,"
            " Company = order.Company"
            "}"
        )


# endregion


class ShipToAndProducts: ...


class OrderProjection: ...


class Total: ...


class EmployeeProjection: ...


class FirstAndLastName: ...


class FullName: ...


class Projections(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_projections(self):
        with self.embedded_server.get_document_store() as store:
            with store.open_session() as session:
                # region projections_1
                results = list(
                    session.query_index_type(Employees_ByFirstAndLastName, Employee).select_fields(
                        Employee, "FirstName", "LastName"
                    )
                )
                # endregion
                # region projections_1_stored
                results = list(
                    session.query_index_type(Employees_ByFirstAndLastNameWithStoredFields, Employee).select_fields(
                        Employee, "FirstName", "LastName"
                    )
                )
                # endregion

                # region projections_2
                query_data = QueryData(["ShipTo", "Lines[].ProductName"], ["ShipTo", "Products"])
                results = list(session.query(object_type=Order).select_fields_query_data(ShipToAndProducts, query_data))
                # endregion

                # region projections_3
                results = list(
                    session.advanced.raw_query(
                        'from Employees as e select { FullName: e.FirstName + " " + e.LastName }', FullName
                    )
                )
                # endregion

                # region projections_4
                results = list(
                    session.advanced.raw_query(
                        "declare function output (e) { "
                        '    var format = function(p){ return p.FirstName + " " + p.LastName; };'
                        "    return { FullName : format(e) }; "
                        "} "
                        "from Employees as e select output(e)",
                        Employee,
                    )
                )
                # endregion

                # region projections_5
                results = list(
                    session.advanced.raw_query(
                        "from Orders as o "
                        "load o.company as c "
                        "select { "
                        "    CompanyName: c.Name,"
                        "    ShippedAt: o.ShippedAt"
                        "}",
                        OrderProjection,
                    )
                )
                # endregion

                # region projections_6
                results = list(
                    session.advanced.raw_query(
                        "from Employees as e "
                        "select { "
                        "    DayOfBirth : new Date(Date.parse(e.Birthday)).getDate(), "
                        "    MonthOfBirth : new Date(Date.parse(e.Birthday)).getMonth() + 1, "
                        "    Age : new Date().getFullYear() - new Date(Date.parse(e.Birthday)).getFullYear() "
                        "}"
                    )
                )
                # endregion

                # region projections_7
                results = list(
                    session.advanced.raw_query(
                        "from Employees as e "
                        "select { "
                        "    Date : new Date(Date.parse(e.Birthday)), "
                        "    Name : e.FirstName.substr(0,3) "
                        "}",
                        EmployeeProjection,
                    )
                )
                # endregion

                # region projections_8
                results = list(
                    session.advanced.raw_query(
                        "from Employee as e " "select {" "    Name : e.FirstName, " "    Metadata : getMetadata(e)" "}",
                        Employee,
                    )
                )
                # endregion
                # region projections_9
                results = session.advanced.raw_query(
                    "from Orders as o "
                    "select { "
                    "    Total : o.Lines.reduce( "
                    "        (acc, 1) => acc += l.PricePerUnit * l.Quantity, 0) "
                    "}",
                    Total,
                )
                # endregion

                # region selectfields_1
                fields = ["Name", "Phone"]
                results = list(
                    session.advanced.document_query_from_index_type(Companies_ByContact, Company).select_fields(
                        ContactDetails, fields
                    )
                )
                # endregion

                # region selectfields_2
                results = list(
                    session.advanced.document_query_from_index_type(Companies_ByContact, Company).select_fields(
                        ContactDetails
                    )
                )
                # endregion


# region index_10
class Companies_ByContact(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "companies.Select(x => new {name = x.Contact.Name, phone = x.Phone})"
        self._store_all_fields(FieldStorage.YES)  # Name and Phone fields can be retrieved directly from index


# endregion


# region projections_10_class
class ContactDetails:
    def __init__(self, name: str = None, phone: str = None):
        self.name = name
        self.phone = phone


# endregion
