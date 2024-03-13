from datetime import datetime, timedelta
from unittest import TestCase
from ravendb import DocumentSession
from ravendb.infrastructure.orders import Order, Address, OrderLine, Company, Employee as RavenEmployee
from ravendb.tools.utils import Utils
from ravendb_embedded import EmbeddedServer, ServerOptions


class Employee(RavenEmployee):
    def to_json(self):
        return {
            "Id": self.Id,
            "last_name": self.last_name,
            "first_name": self.first_name,
            "title": self.title,
            "address": self.address,
            "hired_at": Utils.datetime_to_string(self.hired_at),
            "birthday": Utils.datetime_to_string(self.birthday),
            "home_phone": self.home_phone,
            "extension": self.extension,
            "reports_to": self.reports_to,
            "notes": self.notes,
            "territories": self.territories,
        }


class ExampleBase(TestCase):
    def setUp(self):
        self.embedded_server_port = 8080
        self.embedded_server = EmbeddedServer()
        server_options = ServerOptions()
        server_options.server_url = f"http://127.0.0.1:{self.embedded_server_port}"
        self.embedded_server.start_server(server_options)
        print(server_options.server_url)

    @staticmethod
    def add_orders(session: DocumentSession):
        session.store(
            Order(
                "Funny Order",
                "companies/1",
                "employees/1",
                ship_to=Address(country="Chad"),
                lines=[
                    OrderLine("products/1", "iPhone 15", 199.9, 20),
                    OrderLine("products/2", "Apple Vision Pro", 6666, 2, 0.25),
                ],
            )
        )
        session.store(
            Order(
                "Even More Funny Order",
                "companies/1",
                "employees/3",
                ship_to=Address(country="Poland"),
                lines=[
                    OrderLine("products/3", "Grain", 2, 5000),
                    OrderLine("products/1", "iPhone 15", 1999.9, 500),
                ],
            )
        )
        session.save_changes()

    @staticmethod
    def add_companies(session: DocumentSession):
        session.store(Company("companies/1", name="Stonks Ltd.", address=Address(city="Bristol", country="UK")))
        session.store(
            Company(
                "companies/2",
                name="Black Country New Road Ltd.",
                address=Address(city="Athens", country="France"),
            )
        )
        session.save_changes()

    @staticmethod
    def add_employees(session: DocumentSession):
        session.store(Employee(first_name="John", last_name="Doe", birthday=datetime.today() - timedelta(days=10000)))
        session.store(Employee(first_name="Jane", last_name="Doe", birthday=datetime.today() - timedelta(days=5000)))
        session.save_changes()
