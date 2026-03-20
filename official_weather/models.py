from sqlalchemy import ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.utils import utc_now_iso
from app.modules.device.models import Device  # noqa: F401
from app.modules.household.models import Household  # noqa: F401


class WeatherDeviceBinding(Base):
    """天气插件私有表。"""

    __tablename__ = "weather_device_bindings"
    __table_args__ = (
        UniqueConstraint("device_id", name="uq_weather_device_bindings_device"),
        UniqueConstraint(
            "household_id",
            "binding_key",
            name="uq_weather_device_bindings_household_binding_key",
        ),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    device_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("devices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    household_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("households.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    plugin_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    binding_type: Mapped[str] = mapped_column(String(32), nullable=False)
    binding_key: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    region_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="pending_coordinate", index=True)
    latest_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    cache_expires_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_refresh_attempt_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_success_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False, default=utc_now_iso, onupdate=utc_now_iso)
