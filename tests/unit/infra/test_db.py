from unittest.mock import MagicMock, patch

from prefect_pipeline.infra.db import MongoDB


def test_mongodb_attr_access_returns_collection():
    with patch("prefect_pipeline.infra.db.AsyncIOMotorClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        db = MongoDB("mongodb://u:p@h:27017", "mydb")

        coll = db.my_collection
        # __getattr__ resolves to client["mydb"]["my_collection"]
        expected = mock_client["mydb"]["my_collection"]
        assert coll is expected


def test_mongodb_close_invokes_client_close():
    with patch("prefect_pipeline.infra.db.AsyncIOMotorClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        db = MongoDB("mongodb://u:p@h:27017", "mydb")
        db.close()
        mock_client.close.assert_called_once()
