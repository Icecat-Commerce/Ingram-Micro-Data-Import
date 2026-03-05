"""Session response models for Icecat authentication."""

from pydantic import BaseModel, Field


class SessionData(BaseModel):
    """Session data returned from Icecat login."""

    session_id: str = Field(alias="SessionId")
    user_id: int | None = Field(default=None, alias="UserId")
    user_name: str | None = Field(default=None, alias="UserName")

    model_config = {"populate_by_name": True}


class SessionResponse(BaseModel):
    """Response from Icecat session/login API."""

    data: SessionData | None = Field(default=None, alias="Data")
    message: str | None = Field(default=None, alias="Message")

    model_config = {"populate_by_name": True}
