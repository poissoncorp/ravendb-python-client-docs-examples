from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest import TestCase
from ravendb import DocumentSession
from ravendb.infrastructure.orders import (
    Order as RavenOrder,
    Address as RavenAddress,
    OrderLine as RavenOrderLine,
    Company as RavenCompany,
    Employee as RavenEmployee,
    Product as RavenProduct,
    Contact as RavenContact,
)
from ravendb.tools.utils import Utils
from ravendb_embedded import EmbeddedServer, ServerOptions


class Product(RavenProduct):
    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.Id,
            "Name": self.name,
            "Supplier": self.supplier,
            "Category": self.category,
            "QuantityPerUnit": self.quantity_per_unit,
            "PricePerUnit": self.price_per_unit,
            "UnitsInStock": self.units_in_stock,
            "UnitsOnOrder": self.units_on_order,
            "Discontinued": self.discontinued,
            "ReorderLevel": self.reorder_level,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Product:
        return cls(
            json_dict["Id"],
            json_dict["Name"],
            json_dict["Supplier"],
            json_dict["Category"],
            json_dict["QuantityPerUnit"],
            json_dict["PricePerUnit"],
            json_dict["UnitsInStock"],
            json_dict["UnitsOnOrder"],
            json_dict["Discontinued"],
            json_dict["ReorderLevel"],
        )


class Contact(RavenContact):
    def to_json(self) -> Dict[str, Any]:
        return {"Name": self.name, "Title": self.title}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Contact:
        return cls(json_dict["Name"], json_dict["Title"])


class Company(RavenCompany):
    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.Id,
            "ExternalId": self.external_id,
            "Name": self.name,
            "Contact": self.contact.to_json() if self.contact else None,
            "Address": self.address.to_json() if self.address else None,
            "Phone": self.phone,
            "Fax": self.fax,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Company:
        return cls(
            json_dict["Id"],
            json_dict["ExternalId"],
            json_dict["Name"],
            Contact.from_json(json_dict["Contact"]),
            Address.from_json(json_dict["Address"]),
            json_dict["Phone"],
            json_dict["Fax"],
        )


class OrderLine(RavenOrderLine):
    def to_json(self) -> Dict[str, Any]:
        return {
            "Product": self.product,
            "ProductName": self.product_name,
            "PricePerUnit": self.price_per_unit,
            "Quantity": self.quantity,
            "Discount": self.discount,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> OrderLine:
        return cls(
            json_dict["Product"],
            json_dict["ProductName"],
            json_dict["PricePerUnit"],
            json_dict["Quantity"],
            json_dict["Discount"],
        )


class Order(RavenOrder):
    def to_json(self) -> Dict[str, Any]:
        return {
            "Key": self.key,
            "Company": self.company,
            "Employee": self.employee,
            "OrderedAt": Utils.datetime_to_string(self.ordered_at),
            "RequireAt": Utils.datetime_to_string(self.require_at),
            "ShippedAt": Utils.datetime_to_string(self.shipped_at),
            "ShipTo": self.ship_to.to_json() if self.ship_to is not None else None,
            "ShipVia": self.ship_via,
            "Freight": self.freight,
            "Lines": [line.to_json() for line in self.lines],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Order:
        return cls(
            json_dict["Key"],
            json_dict["Company"],
            json_dict["Employee"],
            Utils.string_to_datetime(json_dict["OrderedAt"]),
            Utils.string_to_datetime(json_dict["RequireAt"]),
            Utils.string_to_datetime(json_dict["ShippedAt"]),
            Address.from_json(json_dict["ShipTo"]),
            json_dict["ShipVia"],
            json_dict["Freight"],
            [OrderLine.from_json(line) for line in json_dict["Lines"]],
        )


class Address(RavenAddress):
    def to_json(self) -> Dict[str, Any]:
        return {
            "Line1": self.line1,
            "Line2": self.line2,
            "City": self.city,
            "Region": self.region,
            "PostalCode": self.postal_code,
            "Country": self.country,
        }

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> Address:
        return cls(
            json["Line1"],
            json["Line2"],
            json["City"],
            json["Region"],
            json["PostalCode"],
            json["Country"],
        )


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

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(
            json_dict["Id"],
            json_dict["LastName"],
            json_dict["FirstName"],
            json_dict["Title"],
            Address.from_json(json_dict["Address"]),
            Utils.string_to_datetime(json_dict["HiredAt"]),
            Utils.string_to_datetime(json_dict["Birthday"]),
            json_dict["HomePhone"],
            json_dict["Extension"],
            json_dict["ReportsTo"],
            json_dict["Notes"],
            json_dict["Territories"],
        )


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
                freight=13.5,
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
        session.store(
            Employee(
                first_name="John", last_name="Doe", birthday=datetime.today() - timedelta(days=10000), notes=["English"]
            )
        )
        session.store(
            Employee(
                first_name="Jane", last_name="Doe", birthday=datetime.today() - timedelta(days=5000), notes=["Italian"]
            )
        )
        session.save_changes()

    @staticmethod
    def add_products(session: DocumentSession):
        session.store(
            Product(
                "products/1",
                "Apple Vision Pro",
                "suppliers/idream",
                "categories/electronics",
                "pcs",
                5000,
                12,
                3,
            )
        )
        session.store(
            Product(
                "products/2",
                "Apple iCar",
                "suppliers/2",
                "categories/automotive",
                "car",
                99000,
                57,
                53,
            )
        )

        session.store(
            Product(
                "products/4",
                "Apple iJet",
                "suppliers/3",
                "categories/aircraft",
                "plane",
                14950000,
                2,
                1,
            )
        )

        session.store(
            Product(
                "products/65",
                "Apple iAirFry",
                "suppliers/5",
                "categories/kitchen",
                "pcs",
                14950,
                52167,
                1,
                True,
            )
        )
        session.store(
            Product(
                "products/65412",
                "Apple iAntimatter",
                "suppliers/x",
                "categories/misc",
                "fg",
                600750399,
                2,
                0,
                True,
            )
        )
        session.save_changes()
