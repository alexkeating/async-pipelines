import aiobotocore
import pytest


def create_client(client_type, request, event_loop, session, region, **kw):
    """Creates aws client

    A generic function that takes in client type and some fixtures
    and returns the appropriate client mocked with moto.

    """

    async def f():
        return session.create_client(client_type, region_name=region, **kw)

    client = event_loop.run_until_complete(f())

    def fin():
        event_loop.run_until_complete(client.close())

    request.addfinalizer(fin)
    return client


@pytest.fixture
def session():
    session = aiobotocore.session.AioSession()
    return session


@pytest.fixture
def region():
    return "us-east-1"


def moto_config(endpoint_url):
    kw = dict(
        endpoint_url=endpoint_url, aws_secret_access_key="xxx", aws_access_key_id="xxx"
    )

    return kw


@pytest.fixture
def sqs_client(request, session, region, sqs_server, event_loop):
    """Creates an sqs client

    request: a pytest fixture

    """
    kw = moto_config(sqs_server)
    client = create_client("sqs", request, event_loop, session, region, **kw)
    return client


pytest_plugins = ["plugins.moto.mock_server", "plugins.sqs"]
