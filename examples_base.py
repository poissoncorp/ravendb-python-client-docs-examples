from ravendb_embedded import EmbeddedServer, ServerOptions


class ExamplesBase:
    def __init__(self, embedded_server_port: int = 8080):
        self.embedded_server_port = embedded_server_port
        self.embedded_server = EmbeddedServer()
        server_options = ServerOptions()
        server_options.server_url = f"http://127.0.0.1:{self.embedded_server_port}"
        self.embedded_server.start_server(server_options)