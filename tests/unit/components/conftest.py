from prefect_pipeline.models import BaseItem
from prefect_pipeline.models.schemas import SchemaBase


class ToyItem(BaseItem):
    """Generic custom item used to verify component generic injection."""

    title: str = ""
    score: int = 0


class ToySchema(SchemaBase):
    """Schema model for LLM extractor tests."""

    summary: str = ""
