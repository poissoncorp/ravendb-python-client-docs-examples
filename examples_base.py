from unittest import TestCase

from ravendb_embedded import EmbeddedServer, ServerOptions


class ExampleBase(TestCase):
    def setUp(self):
        self.embedded_server_port = 8080
        self.embedded_server = EmbeddedServer()
        server_options = ServerOptions()
        server_options.server_url = f"http://127.0.0.1:{self.embedded_server_port}"
        self.embedded_server.start_server(server_options)
        print(server_options.server_url)
