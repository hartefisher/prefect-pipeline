import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_SPEC = """
# Output Schema
<schema_block>
{schema}
</schema_block>

# Output Quality Assurance
You must strictly adhere to these technical constraints to ensure the output is parsable:
1.  **JSON Only**: Return ONLY the JSON object. Do not include any introductory text, markdown code fences (```json), or explanations.
2.  **No Comments**: Do NOT add `//` or `#` comments inside the JSON.
3.  **Valid Syntax**: Ensure curly braces, square brackets are properly closed and commas are correctly placed.
4.  **XML Wrapping**: Wrap the final JSON strictly within `<blocks>` and `</blocks>` tags.

# Result
Generate the response based on the logic and <schema_block> above.
"""


class SchemaBase(BaseModel):
    model_config = ConfigDict(extra="allow")
    error: bool = Field(default=False, exclude=True, description="whether an error occurred, default False")
    index: int = Field(
        default=0,
        exclude=True,
        description="the index position of the element in the list, usually used to mark the order of elements",
    )

    @model_validator(mode="before")
    @classmethod
    def set_defaults_for_none_values(cls, data: Any) -> Any:
        if isinstance(data, dict):
            for field_name, field_value in data.items():
                if field_value is None and field_name in cls.model_fields:
                    data[field_name] = cls.model_fields[field_name].default
        return data

    @classmethod
    def get_instruction(cls, *, annotate_schema: bool = False, **kwargs: Any) -> str:
        if cls.__doc__:
            instruction = cls.__doc__.format(**kwargs)
            if annotate_schema:
                return instruction + "\n" + SCHEMA_SPEC.format(schema=json.dumps(cls.get_model_schema(), indent=2))
            return instruction
        return ""

    @classmethod
    def get_model_schema(cls) -> dict[str, Any]:
        json_schema = cls.model_json_schema()
        if "description" in json_schema:
            del json_schema["description"]
        del json_schema["properties"]["error"]
        del json_schema["properties"]["index"]
        return json_schema


class SummarySchema(SchemaBase):
    summary: str = Field(
        default="",
        description="The summary.",
    )
