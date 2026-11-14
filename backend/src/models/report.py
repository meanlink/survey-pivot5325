from sqlalchemy import String, Double, JSON, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class Report(Base):
    __tablename__ = 'reports'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'))
    destination: Mapped[str] = mapped_column(String(120))
    interval_seconds: Mapped[int]
    next_run: Mapped[float] = mapped_column(Double)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class Delivery(Base):
    """One due distribution run. Both files plus both checksums form the package.

    ``scheduled_for`` is the planned due time that produced this run. It is part
    of the uniqueness key so a restart or a second worker can never archive the
    same run twice.
    """
    __tablename__ = 'deliveries'
    __table_args__ = (UniqueConstraint('report_id', 'scheduled_for', name='uq_delivery_run'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey('reports.id'))
    created_at: Mapped[float] = mapped_column(Double)
    scheduled_for: Mapped[float | None] = mapped_column(Double, nullable=True)
    completed_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    last_attempt_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    # pending = package was claimed but not fully published; complete = both
    # files and checksums were verified before publishing.
    status: Mapped[str] = mapped_column(String(20), default='pending')
    # Legacy columns kept so old archives remain readable.
    checksum: Mapped[str] = mapped_column(String(64))
    filename: Mapped[str] = mapped_column(String(160))
    destination: Mapped[str] = mapped_column(String(120))
    csv_filename: Mapped[str | None] = mapped_column(String(200), nullable=True)
    json_filename: Mapped[str | None] = mapped_column(String(200), nullable=True)
    csv_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    json_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Immutable snapshot of the approved analysis at delivery time. Repair
    # rebuilds from this snapshot, never from a newer analysis.
    snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
