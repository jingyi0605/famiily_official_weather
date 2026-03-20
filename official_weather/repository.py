from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.modules.device.models import DeviceBinding

from .models import WeatherDeviceBinding


def add_weather_device_binding(db: Session, row: WeatherDeviceBinding) -> WeatherDeviceBinding:
    db.add(row)
    return row


def get_weather_device_binding(db: Session, binding_id: str) -> WeatherDeviceBinding | None:
    return db.get(WeatherDeviceBinding, binding_id)


def get_weather_device_binding_for_device(db: Session, *, device_id: str) -> WeatherDeviceBinding | None:
    stmt: Select[tuple[WeatherDeviceBinding]] = select(WeatherDeviceBinding).where(
        WeatherDeviceBinding.device_id == device_id,
    )
    return db.scalar(stmt)


def get_weather_device_binding_for_household_device(
    db: Session,
    *,
    household_id: str,
    device_id: str,
) -> WeatherDeviceBinding | None:
    stmt: Select[tuple[WeatherDeviceBinding]] = select(WeatherDeviceBinding).where(
        WeatherDeviceBinding.household_id == household_id,
        WeatherDeviceBinding.device_id == device_id,
    )
    return db.scalar(stmt)


def get_weather_device_binding_by_key(
    db: Session,
    *,
    household_id: str,
    binding_key: str,
) -> WeatherDeviceBinding | None:
    stmt: Select[tuple[WeatherDeviceBinding]] = select(WeatherDeviceBinding).where(
        WeatherDeviceBinding.household_id == household_id,
        WeatherDeviceBinding.binding_key == binding_key,
    )
    return db.scalar(stmt)


def get_weather_device_binding_for_integration_instance(
    db: Session,
    *,
    integration_instance_id: str,
) -> WeatherDeviceBinding | None:
    stmt: Select[tuple[WeatherDeviceBinding]] = (
        select(WeatherDeviceBinding)
        .join(DeviceBinding, DeviceBinding.device_id == WeatherDeviceBinding.device_id)
        .where(DeviceBinding.integration_instance_id == integration_instance_id)
    )
    return db.scalar(stmt)


def list_weather_device_bindings(
    db: Session,
    *,
    household_id: str,
) -> list[WeatherDeviceBinding]:
    stmt: Select[tuple[WeatherDeviceBinding]] = (
        select(WeatherDeviceBinding)
        .where(WeatherDeviceBinding.household_id == household_id)
        .order_by(WeatherDeviceBinding.updated_at.desc(), WeatherDeviceBinding.id.desc())
    )
    return list(db.scalars(stmt).all())


def delete_weather_device_binding(db: Session, row: WeatherDeviceBinding) -> None:
    db.delete(row)
