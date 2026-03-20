from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

import httpx

from .schemas import (
    WeatherCoordinate,
    WeatherForecastSummary,
    WeatherProviderConfig,
    WeatherProviderType,
    WeatherSnapshot,
)


_MET_SYMBOL_TEXT_MAP = {
    "clearsky_day": "晴",
    "clearsky_night": "晴夜",
    "clearsky_polartwilight": "晴",
    "fair_day": "晴间多云",
    "fair_night": "晴间多云",
    "fair_polartwilight": "晴间多云",
    "partlycloudy_day": "局部多云",
    "partlycloudy_night": "局部多云",
    "partlycloudy_polartwilight": "局部多云",
    "cloudy": "多云",
    "rain": "下雨",
    "rainshowers_day": "阵雨",
    "rainshowers_night": "阵雨",
    "rainshowers_polartwilight": "阵雨",
    "heavyrain": "大雨",
    "lightrain": "小雨",
    "fog": "雾",
    "snow": "下雪",
    "heavysnow": "大雪",
    "lightsnow": "小雪",
    "sleet": "雨夹雪",
    "heavysleet": "强雨夹雪",
    "lightsleet": "小雨夹雪",
}


class WeatherProviderError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        detail: str,
        *,
        field: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(detail)
        self.error_code = error_code
        self.detail = detail
        self.field = field
        self.retryable = retryable


class WeatherProviderAdapter(Protocol):
    provider_type: WeatherProviderType

    def fetch_weather(
        self,
        *,
        coordinate: WeatherCoordinate,
        config: WeatherProviderConfig,
    ) -> WeatherSnapshot: ...


def get_weather_provider(provider_type: WeatherProviderType) -> WeatherProviderAdapter:
    adapters: dict[WeatherProviderType, WeatherProviderAdapter] = {
        "met_norway": MetNorwayAdapter(),
        "openweather": OpenWeatherAdapter(),
        "weatherapi": WeatherApiAdapter(),
    }
    adapter = adapters.get(provider_type)
    if adapter is None:
        raise WeatherProviderError(
            "weather_provider_not_supported",
            f"不支持的天气源: {provider_type}",
            field="provider_type",
        )
    return adapter


class MetNorwayAdapter:
    provider_type: WeatherProviderType = "met_norway"
    _endpoint = "https://api.met.no/weatherapi/locationforecast/2.0/compact"

    def fetch_weather(
        self,
        *,
        coordinate: WeatherCoordinate,
        config: WeatherProviderConfig,
    ) -> WeatherSnapshot:
        payload = _get_json(
            url=self._endpoint,
            params={
                "lat": f"{coordinate.latitude:.6f}",
                "lon": f"{coordinate.longitude:.6f}",
            },
            headers={
                "User-Agent": config.user_agent,
                "Accept": "application/json",
            },
            timeout_seconds=config.request_timeout_seconds,
        )
        properties = _require_dict(payload.get("properties"), "properties")
        timeseries = _require_sequence(properties.get("timeseries"), "timeseries")
        if not timeseries:
            raise WeatherProviderError(
                "weather_provider_response_invalid",
                "MET Norway 没有返回 timeseries 数据。",
                retryable=False,
            )
        current_item = _require_dict(timeseries[0], "timeseries[0]")
        current_data = _require_dict(current_item.get("data"), "timeseries[0].data")
        instant = _require_dict(current_data.get("instant"), "timeseries[0].data.instant")
        instant_details = _require_dict(instant.get("details"), "timeseries[0].data.instant.details")

        condition_code = _read_symbol_code(current_data)
        forecast_6h_code = _read_symbol_code(current_data, key="next_6_hours")
        temperatures = _collect_future_temperatures(timeseries, limit_hours=6)
        updated_at = _normalize_timestamp(
            _read_text(_require_dict(properties.get("meta") or {}, "meta").get("updated_at"))
            or _read_text(current_item.get("time"))
        )
        return WeatherSnapshot(
            source_type=self.provider_type,
            condition_code=condition_code,
            condition_text=_symbol_code_to_text(condition_code),
            temperature=_require_number(instant_details, "air_temperature"),
            humidity=_read_number(instant_details.get("relative_humidity")),
            wind_speed=_read_number(instant_details.get("wind_speed")),
            wind_direction=_read_number(instant_details.get("wind_from_direction")),
            pressure=_read_number(instant_details.get("air_pressure_at_sea_level")),
            cloud_cover=_read_number(instant_details.get("cloud_area_fraction")),
            precipitation_next_1h=_read_precipitation(current_data, key="next_1_hours"),
            forecast_6h=WeatherForecastSummary(
                condition_code=forecast_6h_code,
                condition_text=_symbol_code_to_text(forecast_6h_code),
                min_temperature=min(temperatures) if temperatures else None,
                max_temperature=max(temperatures) if temperatures else None,
            ),
            updated_at=updated_at,
            is_stale=False,
        )


class OpenWeatherAdapter:
    provider_type: WeatherProviderType = "openweather"
    _endpoint = "https://api.openweathermap.org/data/3.0/onecall"

    def fetch_weather(
        self,
        *,
        coordinate: WeatherCoordinate,
        config: WeatherProviderConfig,
    ) -> WeatherSnapshot:
        if not config.openweather_api_key:
            raise WeatherProviderError(
                "weather_provider_key_missing",
                "当前天气源需要 OpenWeather API key。",
                field="openweather_api_key",
            )

        payload = _get_json(
            url=self._endpoint,
            params={
                "lat": f"{coordinate.latitude:.6f}",
                "lon": f"{coordinate.longitude:.6f}",
                "appid": config.openweather_api_key,
                "units": "metric",
                "exclude": "minutely,daily,alerts",
            },
            headers={
                "User-Agent": config.user_agent,
                "Accept": "application/json",
            },
            timeout_seconds=config.request_timeout_seconds,
        )
        current = _require_dict(payload.get("current"), "current")
        hourly = _optional_sequence(payload.get("hourly"))

        current_condition = _read_openweather_condition(current.get("weather"), "current.weather")
        forecast_hours = _collect_openweather_forecast_hours(hourly, limit_hours=6)
        forecast_condition = _resolve_openweather_forecast_condition(forecast_hours, fallback=current_condition)
        forecast_temperatures = _collect_openweather_temperatures(forecast_hours)

        return WeatherSnapshot(
            source_type=self.provider_type,
            condition_code=_build_openweather_condition_code(current_condition),
            condition_text=_read_text(current_condition.get("description"))
            or _read_text(current_condition.get("main"))
            or "未知",
            temperature=_require_number(current, "temp"),
            humidity=_read_number(current.get("humidity")),
            wind_speed=_read_number(current.get("wind_speed")),
            wind_direction=_read_number(current.get("wind_deg")),
            pressure=_read_number(current.get("pressure")),
            cloud_cover=_read_number(current.get("clouds")),
            precipitation_next_1h=_extract_openweather_precipitation(current, first_hour=forecast_hours[0] if forecast_hours else None),
            forecast_6h=WeatherForecastSummary(
                condition_code=_build_openweather_condition_code(forecast_condition),
                condition_text=_read_text(forecast_condition.get("description"))
                or _read_text(forecast_condition.get("main"))
                or "未知",
                min_temperature=min(forecast_temperatures) if forecast_temperatures else None,
                max_temperature=max(forecast_temperatures) if forecast_temperatures else None,
            ),
            updated_at=_normalize_unix_timestamp(_require_int(current.get("dt"), "current.dt")),
            is_stale=False,
        )


class WeatherApiAdapter:
    provider_type: WeatherProviderType = "weatherapi"
    _endpoint = "https://api.weatherapi.com/v1/forecast.json"

    def fetch_weather(
        self,
        *,
        coordinate: WeatherCoordinate,
        config: WeatherProviderConfig,
    ) -> WeatherSnapshot:
        if not config.weatherapi_api_key:
            raise WeatherProviderError(
                "weather_provider_key_missing",
                "当前天气源需要 WeatherAPI key。",
                field="weatherapi_api_key",
            )

        payload = _get_json(
            url=self._endpoint,
            params={
                "key": config.weatherapi_api_key,
                "q": f"{coordinate.latitude:.6f},{coordinate.longitude:.6f}",
                "days": "1",
                "aqi": "no",
                "alerts": "no",
            },
            headers={
                "User-Agent": config.user_agent,
                "Accept": "application/json",
            },
            timeout_seconds=config.request_timeout_seconds,
        )
        current = _require_dict(payload.get("current"), "current")
        current_condition = _require_dict(current.get("condition"), "current.condition")
        current_timestamp = _resolve_weatherapi_current_timestamp(current)

        forecast_hours = _collect_weatherapi_forecast_hours(payload, current_timestamp=current_timestamp, limit_hours=6)
        forecast_condition = _resolve_weatherapi_forecast_condition(forecast_hours, fallback=current_condition)
        forecast_temperatures = _collect_weatherapi_temperatures(forecast_hours)

        wind_kph = _read_number(current.get("wind_kph"))
        return WeatherSnapshot(
            source_type=self.provider_type,
            condition_code=_build_weatherapi_condition_code(current_condition),
            condition_text=_read_text(current_condition.get("text")) or "未知",
            temperature=_require_number(current, "temp_c"),
            humidity=_read_number(current.get("humidity")),
            wind_speed=(wind_kph / 3.6) if wind_kph is not None else None,
            wind_direction=_read_number(current.get("wind_degree")),
            pressure=_read_number(current.get("pressure_mb")),
            cloud_cover=_read_number(current.get("cloud")),
            precipitation_next_1h=_extract_weatherapi_precipitation(current, forecast_hours),
            forecast_6h=WeatherForecastSummary(
                condition_code=_build_weatherapi_condition_code(forecast_condition),
                condition_text=_read_text(forecast_condition.get("text")) or "未知",
                min_temperature=min(forecast_temperatures) if forecast_temperatures else None,
                max_temperature=max(forecast_temperatures) if forecast_temperatures else None,
            ),
            updated_at=_normalize_unix_timestamp(current_timestamp),
            is_stale=False,
        )


def _get_json(
    *,
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    timeout_seconds: int,
) -> dict[str, Any]:
    try:
        with httpx.Client(timeout=timeout_seconds, headers=headers) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
    except httpx.TimeoutException as exc:
        raise WeatherProviderError(
            "weather_provider_timeout",
            "天气源请求超时。",
            retryable=True,
        ) from exc
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:200] if exc.response is not None else ""
        raise WeatherProviderError(
            "weather_provider_http_error",
            f"天气源返回 HTTP {exc.response.status_code if exc.response is not None else 'unknown'}: {detail}",
            retryable=exc.response is None or exc.response.status_code >= 500,
        ) from exc
    except httpx.HTTPError as exc:
        raise WeatherProviderError(
            "weather_provider_http_error",
            f"天气源请求失败: {exc}",
            retryable=True,
        ) from exc

    if not isinstance(payload, dict):
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            "天气源返回的不是对象结构。",
            retryable=False,
        )
    return payload


def _require_dict(payload: Any, name: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            f"{name} 结构无效。",
            retryable=False,
        )
    return payload


def _require_sequence(payload: Any, name: str) -> Sequence[Any]:
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            f"{name} 结构无效。",
            retryable=False,
        )
    return payload


def _optional_sequence(payload: Any) -> Sequence[Any]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return payload
    return []


def _read_symbol_code(data: dict[str, Any], *, key: str = "next_1_hours") -> str:
    candidates = [key, "next_1_hours", "next_6_hours", "next_12_hours"]
    for candidate in candidates:
        payload = data.get(candidate)
        if not isinstance(payload, dict):
            continue
        summary = payload.get("summary")
        if not isinstance(summary, dict):
            continue
        code = _read_text(summary.get("symbol_code"))
        if code:
            return code
    return "unknown"


def _read_precipitation(data: dict[str, Any], *, key: str) -> float | None:
    payload = data.get(key)
    if not isinstance(payload, dict):
        return None
    details = payload.get("details")
    if not isinstance(details, dict):
        return None
    return _read_number(details.get("precipitation_amount"))


def _collect_future_temperatures(timeseries: Sequence[Any], *, limit_hours: int) -> list[float]:
    if not timeseries:
        return []
    first_item = _require_dict(timeseries[0], "timeseries[0]")
    start_time = _parse_time(_read_text(first_item.get("time")))
    if start_time is None:
        return []
    deadline = start_time + timedelta(hours=limit_hours)
    items: list[float] = []
    for raw_item in timeseries:
        item = _require_dict(raw_item, "timeseries.item")
        item_time = _parse_time(_read_text(item.get("time")))
        if item_time is None or item_time > deadline:
            continue
        data = _require_dict(item.get("data"), "timeseries.item.data")
        instant = _require_dict(data.get("instant"), "timeseries.item.data.instant")
        details = _require_dict(instant.get("details"), "timeseries.item.data.instant.details")
        value = _read_number(details.get("air_temperature"))
        if value is None:
            continue
        items.append(value)
    return items


def _read_openweather_condition(payload: Any, name: str) -> dict[str, Any]:
    weather_items = _require_sequence(payload, name)
    if not weather_items:
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            f"{name} 不能为空。",
            retryable=False,
        )
    return _require_dict(weather_items[0], f"{name}[0]")


def _build_openweather_condition_code(condition: dict[str, Any]) -> str:
    weather_id = condition.get("id")
    if isinstance(weather_id, int):
        return f"owm_{weather_id}"
    if isinstance(weather_id, float):
        return f"owm_{int(weather_id)}"
    main = _read_text(condition.get("main"))
    if main:
        return f"owm_{_normalize_identifier(main)}"
    description = _read_text(condition.get("description"))
    if description:
        return f"owm_{_normalize_identifier(description)}"
    return "owm_unknown"


def _collect_openweather_forecast_hours(hourly: Sequence[Any], *, limit_hours: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw_item in hourly:
        if len(items) >= limit_hours:
            break
        items.append(_require_dict(raw_item, "hourly.item"))
    return items


def _resolve_openweather_forecast_condition(
    hours: Sequence[dict[str, Any]],
    *,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for item in hours:
        weather_items = _optional_sequence(item.get("weather"))
        if not weather_items:
            continue
        candidates.append(_require_dict(weather_items[0], "hourly.item.weather[0]"))
    if not candidates:
        return fallback
    counter = Counter(_build_openweather_condition_code(item) for item in candidates)
    dominant_code = counter.most_common(1)[0][0]
    return next((item for item in candidates if _build_openweather_condition_code(item) == dominant_code), fallback)


def _collect_openweather_temperatures(hours: Sequence[dict[str, Any]]) -> list[float]:
    items: list[float] = []
    for item in hours:
        value = _read_number(item.get("temp"))
        if value is not None:
            items.append(value)
    return items


def _extract_openweather_precipitation(
    current: dict[str, Any],
    *,
    first_hour: dict[str, Any] | None,
) -> float | None:
    current_precipitation = _sum_precipitation_parts(current.get("rain"), current.get("snow"))
    if current_precipitation is not None:
        return current_precipitation
    if first_hour is None:
        return None
    hourly_precipitation = _sum_precipitation_parts(first_hour.get("rain"), first_hour.get("snow"))
    if hourly_precipitation is not None:
        return hourly_precipitation
    return 0.0


def _sum_precipitation_parts(*parts: Any) -> float | None:
    total = 0.0
    has_value = False
    for part in parts:
        if isinstance(part, dict):
            for key in ("1h", "3h"):
                value = _read_number(part.get(key))
                if value is not None:
                    total += value
                    has_value = True
    return total if has_value else None


def _resolve_weatherapi_current_timestamp(current: dict[str, Any]) -> int:
    epoch = current.get("last_updated_epoch")
    if isinstance(epoch, int):
        return epoch
    if isinstance(epoch, float):
        return int(epoch)
    last_updated = _read_text(current.get("last_updated"))
    if last_updated is not None:
        try:
            parsed = datetime.strptime(last_updated, "%Y-%m-%d %H:%M")
        except ValueError as exc:
            raise WeatherProviderError(
                "weather_provider_response_invalid",
                "WeatherAPI 缺少合法更新时间。",
                retryable=False,
            ) from exc
        return int(parsed.replace(tzinfo=timezone.utc).timestamp())
    raise WeatherProviderError(
        "weather_provider_response_invalid",
        "WeatherAPI 缺少更新时间。",
        retryable=False,
    )


def _collect_weatherapi_forecast_hours(
    payload: dict[str, Any],
    *,
    current_timestamp: int,
    limit_hours: int,
) -> list[dict[str, Any]]:
    forecast = _require_dict(payload.get("forecast"), "forecast")
    forecastdays = _require_sequence(forecast.get("forecastday"), "forecast.forecastday")
    if not forecastdays:
        return []
    first_day = _require_dict(forecastdays[0], "forecast.forecastday[0]")
    hours = _require_sequence(first_day.get("hour"), "forecast.forecastday[0].hour")
    items: list[dict[str, Any]] = []
    for raw_item in hours:
        item = _require_dict(raw_item, "forecast.forecastday[0].hour.item")
        item_epoch = _read_epoch(item.get("time_epoch"))
        if item_epoch is None or item_epoch < current_timestamp:
            continue
        items.append(item)
        if len(items) >= limit_hours:
            break
    return items


def _resolve_weatherapi_forecast_condition(
    hours: Sequence[dict[str, Any]],
    *,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for item in hours:
        condition = item.get("condition")
        if not isinstance(condition, dict):
            continue
        candidates.append(condition)
    if not candidates:
        return fallback
    counter = Counter(_build_weatherapi_condition_code(item) for item in candidates)
    dominant_code = counter.most_common(1)[0][0]
    return next((item for item in candidates if _build_weatherapi_condition_code(item) == dominant_code), fallback)


def _collect_weatherapi_temperatures(hours: Sequence[dict[str, Any]]) -> list[float]:
    items: list[float] = []
    for item in hours:
        value = _read_number(item.get("temp_c"))
        if value is not None:
            items.append(value)
    return items


def _build_weatherapi_condition_code(condition: dict[str, Any]) -> str:
    code = condition.get("code")
    if isinstance(code, int):
        return f"weatherapi_{code}"
    if isinstance(code, float):
        return f"weatherapi_{int(code)}"
    text = _read_text(condition.get("text"))
    if text:
        return f"weatherapi_{_normalize_identifier(text)}"
    return "weatherapi_unknown"


def _extract_weatherapi_precipitation(current: dict[str, Any], forecast_hours: Sequence[dict[str, Any]]) -> float | None:
    if forecast_hours:
        value = _read_number(forecast_hours[0].get("precip_mm"))
        if value is not None:
            return value
    return _read_number(current.get("precip_mm"))


def _symbol_code_to_text(symbol_code: str) -> str:
    normalized = symbol_code.strip().lower()
    if not normalized:
        return "未知"
    return _MET_SYMBOL_TEXT_MAP.get(normalized, normalized.replace("_", " "))


def _require_number(payload: dict[str, Any], key: str) -> float:
    value = _read_number(payload.get(key))
    if value is None:
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            f"天气源缺少必需字段 {key}。",
            retryable=False,
        )
    return value


def _require_int(value: Any, name: str) -> int:
    epoch = _read_epoch(value)
    if epoch is None:
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            f"天气源缺少合法字段 {name}。",
            retryable=False,
        )
    return epoch


def _read_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _read_epoch(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _read_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _normalize_timestamp(value: str | None) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        raise WeatherProviderError(
            "weather_provider_response_invalid",
            "天气源缺少合法更新时间。",
            retryable=False,
        )
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_unix_timestamp(value: int) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_identifier(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    return "".join(char for char in normalized if char.isalnum() or char == "_") or "unknown"
