"""
Database models.
"""
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import relationship

from app.database import Base
from app.utils.time_utils import UTCDateTime, get_now, get_app_timezone


LEGACY_APP_TZ = get_app_timezone()
TEAM_MEMBER_LIMIT = 5


class Team(Base):
    """Team information."""

    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, comment="Team owner email")
    access_token_encrypted = Column(Text, nullable=False, comment="Encrypted access token")
    refresh_token_encrypted = Column(Text, comment="Encrypted refresh token")
    session_token_encrypted = Column(Text, comment="Encrypted session token")
    client_id = Column(String(100), comment="OAuth client ID")
    encryption_key_id = Column(String(50), comment="Encryption key ID")
    account_id = Column(String(100), comment="Current account ID")
    team_name = Column(String(255), comment="Team name")
    plan_type = Column(String(50), comment="Plan type")
    subscription_plan = Column(String(100), comment="Subscription plan")
    expires_at = Column(UTCDateTime(), comment="Subscription expiration time")
    current_members = Column(Integer, default=0, comment="Current member count")
    max_members = Column(Integer, default=TEAM_MEMBER_LIMIT, comment="Maximum member count")
    status = Column(String(20), default="active", comment="active/full/expired/error/banned")
    account_role = Column(String(50), comment="Account role")
    device_code_auth_enabled = Column(Boolean, default=False, comment="Device code auth enabled")
    error_count = Column(Integer, default=0, comment="Consecutive error count")
    last_sync = Column(UTCDateTime(LEGACY_APP_TZ), comment="Last sync time")
    created_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, comment="Created at")

    team_accounts = relationship("TeamAccount", back_populates="team", cascade="all, delete-orphan")
    redemption_records = relationship("RedemptionRecord", back_populates="team", cascade="all, delete-orphan")

    __table_args__ = (Index("idx_status", "status"),)


class TeamAccount(Base):
    """Team account mapping."""

    __tablename__ = "team_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    account_id = Column(String(100), nullable=False, comment="Account ID")
    account_name = Column(String(255), comment="Account name")
    is_primary = Column(Boolean, default=False, comment="Primary account")
    created_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, comment="Created at")

    team = relationship("Team", back_populates="team_accounts")

    __table_args__ = (Index("idx_team_account", "team_id", "account_id", unique=True),)


class RedemptionCode(Base):
    """Redemption code."""

    __tablename__ = "redemption_codes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(32), unique=True, nullable=False, comment="Redemption code")
    status = Column(
        String(20),
        default="unused",
        comment="unused/invite_pending/used/expired/warranty_active",
    )
    created_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, comment="Created at")
    expires_at = Column(UTCDateTime(LEGACY_APP_TZ), comment="Code expiration time")
    used_by_email = Column(String(255), comment="Used by email")
    used_team_id = Column(Integer, ForeignKey("teams.id"), comment="Used team ID")
    used_at = Column(UTCDateTime(LEGACY_APP_TZ), comment="Used at")
    has_warranty = Column(Boolean, default=False, comment="Whether warranty is enabled")
    warranty_days = Column(Integer, default=30, comment="Warranty duration in days")
    warranty_expires_at = Column(UTCDateTime(LEGACY_APP_TZ), comment="Warranty expiration time")

    redemption_records = relationship("RedemptionRecord", back_populates="redemption_code")

    __table_args__ = (Index("idx_code_status", "code", "status"),)


class RedemptionRecord(Base):
    """Redemption usage record."""

    __tablename__ = "redemption_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, comment="User email")
    code = Column(String(32), ForeignKey("redemption_codes.code"), nullable=False, comment="Code")
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, comment="Team ID")
    account_id = Column(String(100), nullable=False, comment="Account ID")
    redeemed_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, comment="Redeemed at")
    is_warranty_redemption = Column(Boolean, default=False, comment="Warranty redemption")

    team = relationship("Team", back_populates="redemption_records")
    redemption_code = relationship("RedemptionCode", back_populates="redemption_records")

    __table_args__ = (Index("idx_email", "email"),)


class Setting(Base):
    """Application setting."""

    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), unique=True, nullable=False, comment="Setting key")
    value = Column(Text, comment="Setting value")
    description = Column(String(255), comment="Setting description")
    created_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, comment="Created at")
    updated_at = Column(UTCDateTime(LEGACY_APP_TZ), default=get_now, onupdate=get_now, comment="Updated at")

    __table_args__ = (Index("idx_key", "key"),)
