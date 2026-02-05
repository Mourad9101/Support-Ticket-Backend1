import httpx


class PatchedAsyncClient(httpx.AsyncClient):
    def __init__(self, *args, app=None, **kwargs):
        if app is not None and "transport" not in kwargs:
            kwargs["transport"] = httpx.ASGITransport(app=app)
            kwargs.setdefault("base_url", "http://test")
        super().__init__(*args, **kwargs)


httpx.AsyncClient = PatchedAsyncClient
