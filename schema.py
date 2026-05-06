from pydantic import BaseModel, Field


class FileUrl(BaseModel):
    name: str
    url: str


class BatchApplyResponse(BaseModel):
    batch_id: str
    file_urls: list[str]


class ExtractResult(BaseModel):
    file_id: str
    status: str
    full_zip_url: str | None = Field(default=None)
    extract_result: dict | None = Field(default=None)
