from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


WeatherProviderType = Literal["met_norway", "openweather", "weatherapi"]
WeatherBindingType = Literal["default_household", "region_node"]
WeatherDeviceCardState = Literal["ready", "stale", "pending_coordinate", "error"]

WEATHER_PROVIDER_DEFAULT_TYPE: WeatherProviderType = "met_norway"
WEATHER_PROVIDER_DEFAULT_TIMEOUT_SECONDS = 10
WEATHER_PROVIDER_DEFAULT_REFRESH_INTERVAL_MINUTES = 30
WEATHER_PROVIDER_DEFAULT_USER_AGENT = "FamilyClaw/0.1 (official-weather-plugin)"


class WeatherCoordinate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class WeatherForecastSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    condition_code: str
    condition_text: str
    min_temperature: float | None = None
    max_temperature: float | None = None


class WeatherSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: WeatherProviderType
    condition_code: str
    condition_text: str
    temperature: float
    humidity: float | None = None
    wind_speed: float | None = None
    wind_direction: float | None = None
    pressure: float | None = None
    cloud_cover: float | None = None
    precipitation_next_1h: float | None = None
    forecast_6h: WeatherForecastSummary | None = None
    updated_at: str
    is_stale: bool = False


class WeatherBindingCoordinateResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available: bool
    coordinate: WeatherCoordinate | None = None
    error_code: str | None = None
    detail: str | None = None

    @model_validator(mode="after")
    def validate_coordinate_resolution(self) -> "WeatherBindingCoordinateResolution":
        if self.available and self.coordinate is None:
            raise ValueError("available=true 时必须带 coordinate")
        if not self.available and self.coordinate is not None:
            raise ValueError("available=false 时不能带 coordinate")
        return self


class WeatherProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_type: WeatherProviderType = WEATHER_PROVIDER_DEFAULT_TYPE
    refresh_interval_minutes: int = Field(
        default=WEATHER_PROVIDER_DEFAULT_REFRESH_INTERVAL_MINUTES,
        ge=5,
        le=240,
    )
    request_timeout_seconds: int = Field(
        default=WEATHER_PROVIDER_DEFAULT_TIMEOUT_SECONDS,
        ge=3,
        le=60,
    )
    user_agent: str = Field(
        default=WEATHER_PROVIDER_DEFAULT_USER_AGENT,
        min_length=1,
        max_length=255,
    )
    openweather_api_key: str | None = Field(default=None, min_length=1, max_length=255)
    weatherapi_api_key: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def normalize_secret_requirements(self) -> "WeatherProviderConfig":
        if self.provider_type == "openweather" and not self.openweather_api_key:
            raise ValueError("openweather_api_key 不能为空")
        if self.provider_type == "weatherapi" and not self.weatherapi_api_key:
            raise ValueError("weatherapi_api_key 不能为空")
        return self


class WeatherDeviceBindingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding_type: Literal["region_node"] = "region_node"
    display_name: str | None = Field(default=None, min_length=1, max_length=100)
    provider_code: str = Field(min_length=1, max_length=64)
    region_code: str = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_binding_payload(self) -> "WeatherDeviceBindingCreate":
        if self.binding_type != "region_node":
            raise ValueError("当前版本只支持 region_node 类型天气设备")
        return self


class WeatherDeviceBindingRead(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device_id: str
    household_id: str
    plugin_id: str
    binding_type: WeatherBindingType
    binding_key: str
    display_name: str
    provider_code: str | None = None
    region_code: str | None = None
    state: str
    last_refresh_attempt_at: str | None = None
    last_success_at: str | None = None
    last_error_code: str | None = None
    last_error_message: str | None = None
    created_at: str
    updated_at: str


class WeatherDeviceCardSnapshotRead(BaseModel):
    model_config = ConfigDict(extra="forbid")

    card_key: str
    device_id: str
    display_name: str
    state: WeatherDeviceCardState
    payload: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    generated_at: str | None = None
    expires_at: str | None = None
