from __future__ import annotations
import json
from datetime import datetime, timezone
from flask import current_app
from marshmallow import Schema, fields
from sqlalchemy import text
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_
from swpt_debtors.extensions import db, publisher, DEBTORS_OUT_EXCHANGE
from swpt_pythonlib import rabbitmq

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
T_INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
DATE0 = TS0.date()
HUGE_NEGLIGIBLE_AMOUNT = 1e30
TRANSFER_NOTE_MAX_BYTES = 500
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
CONFIG_DATA_MAX_BYTES = 2000
ROOT_CREDITOR_ID = 0
DEFAULT_CONFIG_FLAGS = 0
SET_SEQSCAN_ON = text("SET LOCAL enable_seqscan = on")
DISCARD_PLANS = text("DISCARD PLANS")

CT_ISSUING = "issuing"

SC_OK = "OK"
SC_UNEXPECTED_ERROR = "UNEXPECTED_ERROR"
SC_INSUFFICIENT_AVAILABLE_AMOUNT = "INSUFFICIENT_AVAILABLE_AMOUNT"
SC_CANCELED_BY_THE_SENDER = "CANCELED_BY_THE_SENDER"


def is_valid_debtor_id(debtor_id: int, match_parent=False) -> bool:
    sharding_realm = current_app.config["SHARDING_REALM"]
    min_debtor_id = current_app.config["MIN_DEBTOR_ID"]
    max_debtor_id = current_app.config["MAX_DEBTOR_ID"]
    return (
        min_debtor_id <= debtor_id <= max_debtor_id
        and sharding_realm.match(debtor_id, match_parent=match_parent)
    )


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class ChooseRowsMixin:
    @classmethod
    def choose_rows(cls, primary_keys: list[tuple], name: str = "chosen"):
        pktype_name = f"{cls.__table__.name}_pktype"
        bindparam_name = f"{name}_rows"
        return (
            text(f"SELECT * FROM unnest(:{bindparam_name} :: {pktype_name}[])")
            .bindparams(**{bindparam_name: primary_keys})
            .columns(**{c.key: c.type for c in inspect(cls).primary_key})
            .cte(name=name)
        )


class Signal(db.Model, ChooseRowsMixin):
    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        create_message = cls._create_message
        messages = (create_message(obj) for obj in objects)
        publisher.publish_messages([m for m in messages if m is not None])

    @classmethod
    def send_signalbus_message(cls, obj):  # pragma: no cover
        cls.send_signalbus_messages([obj])

    @classmethod
    def _create_message(cls, obj):  # pragma: no cover
        data = cls.__marshmallow_schema__.dump(obj)
        message_type = data["type"]
        creditor_id = data["creditor_id"]
        debtor_id = data["debtor_id"]

        if not is_valid_debtor_id(debtor_id):
            if current_app.config[
                "DELETE_PARENT_SHARD_RECORDS"
            ] and is_valid_debtor_id(debtor_id, match_parent=True):
                # This message most probably is a left-over from the
                # previous splitting of the parent shard into children
                # shards. Therefore we should just ignore it.
                return None
            raise RuntimeError("The agent is not responsible for this debtor.")

        headers = {
            "message-type": message_type,
            "debtor-id": debtor_id,
            "creditor-id": creditor_id,
        }
        if "coordinator_id" in data:
            headers["coordinator-id"] = data["coordinator_id"]
            headers["coordinator-type"] = data["coordinator_type"]

        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id="swpt_debtors",
            content_type="application/json",
            type=message_type,
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf8")

        return rabbitmq.Message(
            exchange=cls.exchange_name,
            routing_key=cls.routing_key,
            body=body,
            properties=properties,
            mandatory=True,
        )

    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )


class Debtor(db.Model, ChooseRowsMixin):
    STATUS_IS_ACTIVATED_FLAG = 1 << 0
    STATUS_IS_DEACTIVATED_FLAG = 1 << 1

    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    _ad_seq = db.Sequence(
        "debtor_reservation_id_seq", metadata=db.Model.metadata
    )

    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The `status_flags` column is not be part of the primary
    # key, but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2020-01-11), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.
    status_flags = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "Debtor's status bits: "
            f"{STATUS_IS_ACTIVATED_FLAG} - is activated, "
            f"{STATUS_IS_DEACTIVATED_FLAG} - is deactivated."
        ),
    )

    deactivation_date = db.Column(
        db.DATE,
        comment=(
            "The date on which the debtor was deactivated. When a debtor gets"
            " deactivated, all its belonging objects (transfers, etc.) are"
            " removed. To be deactivated, the debtor must be activated first."
            " Once deactivated, a debtor stays deactivated until it is"
            " deleted. A `NULL` value for this column means either that the"
            " debtor has not been deactivated yet, or that the deactivation"
            " date is unknown."
        ),
    )
    reservation_id = db.Column(
        db.BigInteger, server_default=_ad_seq.next_value()
    )
    created_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    balance = db.Column(db.BigInteger, nullable=False, default=0)
    min_balance = db.Column(db.BigInteger, nullable=False, default=MIN_INT64)
    transfer_note_max_bytes = db.Column(db.Integer, nullable=False, default=0)
    running_transfers_count = db.Column(db.Integer, nullable=False, default=0)
    actions_count = db.Column(db.Integer, nullable=False, default=0)
    actions_count_reset_date = db.Column(
        db.DATE, nullable=False, default=get_now_utc
    )
    documents_count = db.Column(db.Integer, nullable=False, default=0)
    documents_count_reset_date = db.Column(
        db.DATE, nullable=False, default=get_now_utc
    )
    has_server_account = db.Column(db.BOOLEAN, nullable=False, default=False)
    account_creation_date = db.Column(db.DATE, nullable=False, default=DATE0)
    account_last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    account_last_change_seqnum = db.Column(
        db.Integer, nullable=False, default=0
    )
    account_last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    account_id = db.Column(db.String, nullable=False, default="")
    last_config_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    last_config_seqnum = db.Column(db.Integer, nullable=False, default=0)
    is_config_effectual = db.Column(db.BOOLEAN, nullable=False, default=False)
    config_flags = db.Column(
        db.Integer, nullable=False, default=DEFAULT_CONFIG_FLAGS
    )
    config_data = db.Column(db.String, nullable=False, default="")
    config_error = db.Column(db.String)
    config_latest_update_id = db.Column(
        db.BigInteger, nullable=False, default=1
    )
    debtor_info_iri = db.Column(db.String)

    __mapper_args__ = {
        "eager_defaults": True,
    }
    __table_args__ = (
        db.CheckConstraint(config_latest_update_id > 0),
        db.CheckConstraint(
            or_(
                status_flags.op("&")(STATUS_IS_DEACTIVATED_FLAG) == 0,
                status_flags.op("&")(STATUS_IS_ACTIVATED_FLAG) != 0,
            )
        ),
        db.CheckConstraint(
            or_(
                deactivation_date == null(),
                status_flags.op("&")(STATUS_IS_DEACTIVATED_FLAG) != 0,
            )
        ),
        db.CheckConstraint(actions_count >= 0),
        db.CheckConstraint(min_balance <= 0),
    )

    @property
    def is_activated(self):
        return bool(self.status_flags & Debtor.STATUS_IS_ACTIVATED_FLAG)

    @property
    def is_deactivated(self):
        return bool(self.status_flags & Debtor.STATUS_IS_DEACTIVATED_FLAG)

    def activate(self):
        self.status_flags |= Debtor.STATUS_IS_ACTIVATED_FLAG

    def deactivate(self):
        self.status_flags |= Debtor.STATUS_IS_DEACTIVATED_FLAG
        self.deactivation_date = datetime.now(tz=timezone.utc).date()
        self.is_config_effectual = True
        self.config_flags = (
            DEFAULT_CONFIG_FLAGS | self.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        )
        self.config_data = ""
        self.config_error = None
        self.debtor_info_iri = None


class RunningTransfer(db.Model):
    _cr_seq = db.Sequence(
        "coordinator_request_id_seq", metadata=db.Model.metadata
    )

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    recipient_uri = db.Column(db.String, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    initiated_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    finalized_at = db.Column(db.TIMESTAMP(timezone=True))
    error_code = db.Column(db.String)
    total_locked_amount = db.Column(db.BigInteger)
    coordinator_request_id = db.Column(
        db.BigInteger, nullable=False, server_default=_cr_seq.next_value()
    )
    transfer_id = db.Column(db.BigInteger)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["debtor_id"], ["debtor.debtor_id"], ondelete="CASCADE"
        ),
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(total_locked_amount >= 0),
        db.CheckConstraint(or_(error_code == null(), finalized_at != null())),
        db.Index(
            "idx_coordinator_request_id",
            debtor_id,
            coordinator_request_id,
            unique=True,
        ),
        {
            "comment": (
                "Represents an initiated issuing transfer. A new row is"
                " inserted when a debtor creates a new issuing transfer. The"
                " row is deleted when the debtor acknowledges (purges) the"
                " transfer."
            ),
        },
    )

    @property
    def is_settled(self):
        return self.transfer_id is not None

    @property
    def is_finalized(self):
        return bool(self.finalized_at)


class Document(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    document_id = db.Column(
        db.BigInteger, primary_key=True, autoincrement=True
    )
    content_type = db.Column(db.String, nullable=False)
    content = db.Column(db.LargeBinary, nullable=False)
    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["debtor_id"], ["debtor.debtor_id"], ondelete="CASCADE"
        ),
        {
            "comment": (
                "Represents a document saved by the debtor, which should"
                " remain available indefinitely, or at least for a very long"
                " time."
            ),
        },
    )


class ConfigureAccountSignal(Signal):
    exchange_name = DEBTORS_OUT_EXCHANGE
    routing_key = ""

    class __marshmallow__(Schema):
        type = fields.Constant("ConfigureAccount")
        debtor_id = fields.Integer()
        creditor_id = fields.Constant(ROOT_CREDITOR_ID)
        ts = fields.DateTime()
        seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    config_data = db.Column(db.String, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT"]


class PrepareTransferSignal(Signal):
    exchange_name = DEBTORS_OUT_EXCHANGE
    routing_key = ""

    class __marshmallow__(Schema):
        type = fields.Constant("PrepareTransfer")
        coordinator_type = fields.String(dump_default=CT_ISSUING)
        coordinator_id = fields.Integer(attribute="debtor_id", dump_only=True)
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer(attribute="amount", dump_only=True)
        max_locked_amount = fields.Integer(attribute="amount", dump_only=True)
        debtor_id = fields.Integer()
        creditor_id = fields.Constant(ROOT_CREDITOR_ID)
        recipient = fields.String()
        inserted_at = fields.DateTime(data_key="ts")
        max_commit_delay = fields.Constant(MAX_INT32)
        final_interest_rate_ts = fields.DateTime()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    __table_args__ = (db.CheckConstraint(amount >= 0),)

    @property
    def final_interest_rate_ts(self) -> datetime:
        return T_INFINITY  # pragma: no cover

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT"]


class FinalizeTransferSignal(Signal):
    exchange_name = DEBTORS_OUT_EXCHANGE
    routing_key = ""

    class __marshmallow__(Schema):
        type = fields.Constant("FinalizeTransfer")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.String(dump_default=CT_ISSUING)
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (db.CheckConstraint(committed_amount >= 0),)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT"]
