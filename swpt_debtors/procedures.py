import json
from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, List, Tuple, Dict, Any
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import func
from swpt_pythonlib.utils import Seqnum, increment_seqnum
from swpt_debtors.extensions import db
from swpt_debtors.models import (
    Debtor,
    FinalizeTransferSignal,
    RunningTransfer,
    ConfigureAccountSignal,
    PrepareTransferSignal,
    Document,
    MAX_INT32,
    ROOT_CREDITOR_ID,
    DEFAULT_CONFIG_FLAGS,
    HUGE_NEGLIGIBLE_AMOUNT,
    SC_UNEXPECTED_ERROR,
    SC_CANCELED_BY_THE_SENDER,
    SC_OK,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

STATUS_FLAGS_MASK = (
    Debtor.STATUS_IS_ACTIVATED_FLAG | Debtor.STATUS_IS_DEACTIVATED_FLAG
)
TD_SECOND = timedelta(seconds=1)
EPS = 1e-5


class UpdateConflict(Exception):
    """A conflict occurred while trying to update a resource."""


class AlreadyUpToDate(Exception):
    """Trying to update a resource which is already up-to-date."""


class MisconfiguredNode(Exception):
    """The node is misconfigured."""


class InvalidReservationId(Exception):
    """Invalid debtor reservation ID."""


class DebtorDoesNotExist(Exception):
    """The debtor does not exist."""


class DebtorExists(Exception):
    """The same debtor record already exists."""


class TransferDoesNotExist(Exception):
    """The transfer does not exist."""


class TransferExists(Exception):
    """The same initiated transfer record already exists."""


class TransfersConflict(Exception):
    """A different transfer with conflicting UUID already exists."""


class TooManyRunningTransfers(Exception):
    """Too many initiated, not deleted transfers."""


class ForbiddenTransferCancellation(Exception):
    """The transfer can not be canceled."""


class TooManyManagementActions(Exception):
    """Too many management actions per month by a debtor."""


class TooManySavedDocuments(Exception):
    """Too many saved documents per year by a debtor."""


@atomic
def get_debtor_ids(
    start_from: int, count: int = 1
) -> Tuple[List[int], Optional[int]]:
    assert count >= 1
    query = (
        db.session.query(Debtor.debtor_id)
        .filter(Debtor.debtor_id >= start_from)
        .filter(
            Debtor.status_flags.op("&")(STATUS_FLAGS_MASK)
            == Debtor.STATUS_IS_ACTIVATED_FLAG
        )
        .order_by(Debtor.debtor_id)
        .limit(count + 1)
    )
    debtor_ids = [t[0] for t in query.all()]

    if len(debtor_ids) > count:
        next_debtor_id = debtor_ids.pop()
    else:
        next_debtor_id = None

    return debtor_ids, next_debtor_id


@atomic
def reserve_debtor(debtor_id) -> Debtor:
    debtor = Debtor(debtor_id=debtor_id)
    db.session.add(debtor)
    try:
        db.session.flush()
    except IntegrityError:
        raise DebtorExists() from None

    return debtor


@atomic
def activate_debtor(debtor_id: int, reservation_id: str) -> Debtor:
    debtor = get_debtor(debtor_id, lock=True)
    if debtor is None:
        raise InvalidReservationId()

    if reservation_id == str(debtor.reservation_id):
        if not debtor.is_activated:
            debtor.activate()
            _insert_configure_account_signal(debtor)
    elif debtor.is_activated:
        raise DebtorExists()  # pragma: no cover
    else:
        raise InvalidReservationId()

    return debtor


@atomic
def deactivate_debtor(debtor_id: int, deleted_account: bool = False) -> None:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor:
        debtor.deactivate()
        _insert_configure_account_signal(debtor)
        _delete_debtor_transfers(debtor)


@atomic
def restrict_debtor(debtor_id: int, min_balance: int) -> Debtor:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        raise DebtorDoesNotExist()

    debtor.min_balance = min_balance
    _insert_configure_account_signal(debtor)

    return debtor


@atomic
def get_debtor(
    debtor_id: int, *, lock: bool = False, active: bool = False
) -> Optional[Debtor]:
    query = Debtor.query.filter_by(debtor_id=debtor_id)
    if active:
        query = query.filter(
            Debtor.status_flags.op("&")(STATUS_FLAGS_MASK)
            == Debtor.STATUS_IS_ACTIVATED_FLAG
        )
    if lock:
        query = query.with_for_update(key_share=True)

    return query.one_or_none()


@atomic
def get_active_debtor(debtor_id: int, lock: bool = False) -> Optional[Debtor]:
    return get_debtor(debtor_id, lock=lock, active=True)


@atomic
def update_debtor_config(
    debtor_id: int,
    *,
    config_data: str,
    latest_update_id: int,
    max_actions_per_month: int = MAX_INT32
) -> Debtor:
    current_ts = datetime.now(tz=timezone.utc)
    debtor = _throttle_debtor_actions(
        debtor_id, max_actions_per_month, current_ts
    )
    try:
        perform_update = _allow_update(
            debtor,
            "config_latest_update_id",
            latest_update_id,
            {
                "config_data": config_data,
            },
        )
    except AlreadyUpToDate:
        return debtor

    perform_update()

    _insert_configure_account_signal(debtor)
    return debtor


@atomic
def get_debtor_transfer_uuids(debtor_id: int) -> List[UUID]:
    debtor = get_active_debtor(debtor_id)
    if debtor is None:
        raise DebtorDoesNotExist()

    rows = (
        db.session.query(RunningTransfer.transfer_uuid)
        .filter_by(debtor_id=debtor_id)
        .all()
    )

    return [uuid for (uuid,) in rows]


@atomic
def get_running_transfer(
    debtor_id: int, transfer_uuid: UUID, lock=False
) -> Optional[RunningTransfer]:
    query = RunningTransfer.query.filter_by(
        debtor_id=debtor_id, transfer_uuid=transfer_uuid
    )
    if lock:
        query = query.with_for_update(key_share=True)

    return query.one_or_none()


@atomic
def cancel_running_transfer(
    debtor_id: int, transfer_uuid: UUID
) -> RunningTransfer:
    rt = get_running_transfer(debtor_id, transfer_uuid, lock=True)
    if rt is None:
        raise TransferDoesNotExist()

    if rt.is_settled:
        raise ForbiddenTransferCancellation()

    _finalize_running_transfer(rt, error_code=SC_CANCELED_BY_THE_SENDER)
    return rt


@atomic
def delete_running_transfer(debtor_id: int, transfer_uuid: UUID) -> None:
    number_of_deleted_rows = RunningTransfer.query.filter_by(
        debtor_id=debtor_id, transfer_uuid=transfer_uuid
    ).delete(synchronize_session=False)

    if number_of_deleted_rows == 0:
        raise TransferDoesNotExist()

    assert number_of_deleted_rows == 1
    Debtor.query.filter_by(debtor_id=debtor_id).update(
        {Debtor.running_transfers_count: Debtor.running_transfers_count - 1},
        synchronize_session=False,
    )


@atomic
def initiate_running_transfer(
    debtor_id: int,
    transfer_uuid: UUID,
    recipient_uri: str,
    recipient: str,
    amount: int,
    transfer_note_format: str,
    transfer_note: str,
    max_actions_per_month: int = MAX_INT32,
) -> RunningTransfer:
    current_ts = datetime.now(tz=timezone.utc)
    transfer_data = {
        "amount": amount,
        "recipient_uri": recipient_uri,
        "recipient": recipient,
        "transfer_note_format": transfer_note_format,
        "transfer_note": transfer_note,
    }

    rt = get_running_transfer(debtor_id, transfer_uuid)
    if rt:
        if any(
            getattr(rt, attr) != value for attr, value in transfer_data.items()
        ):
            raise TransfersConflict()
        raise TransferExists()

    debtor = _throttle_debtor_actions(
        debtor_id, max_actions_per_month, current_ts
    )
    debtor.running_transfers_count += 1
    if debtor.running_transfers_count > max_actions_per_month:
        raise TooManyRunningTransfers()

    new_running_transfer = RunningTransfer(
        debtor_id=debtor_id,
        transfer_uuid=transfer_uuid,
        **transfer_data,
    )
    with db.retry_on_integrity_error():
        db.session.add(new_running_transfer)

    db.session.add(
        PrepareTransferSignal(
            debtor_id=debtor_id,
            coordinator_request_id=new_running_transfer.coordinator_request_id,
            amount=amount,
            recipient=recipient,
        )
    )

    return new_running_transfer


@atomic
def process_rejected_config_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    config_ts: datetime,
    config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    rejection_code: str
) -> None:
    if creditor_id != ROOT_CREDITOR_ID:  # pragma: no cover
        return

    debtor = (
        Debtor.query.filter_by(
            debtor_id=debtor_id,
            last_config_ts=config_ts,
            last_config_seqnum=config_seqnum,
            config_flags=config_flags,
            config_data=config_data,
            config_error=None,
        )
        .filter(
            func.abs(Debtor.min_balance + negligible_amount)
            <= EPS * negligible_amount
        )
        .with_for_update(key_share=True)
        .one_or_none()
    )

    if debtor:
        debtor.config_error = rejection_code


@atomic
def process_account_purge_signal(
    *, debtor_id: int, creditor_id: int, creation_date: date
) -> None:
    if creditor_id != ROOT_CREDITOR_ID:  # pragma: no cover
        return

    debtor = (
        Debtor.query.filter_by(
            debtor_id=debtor_id,
            has_server_account=True,
        )
        .filter(Debtor.account_creation_date <= creation_date)
        .with_for_update(key_share=True)
        .one_or_none()
    )

    if debtor:
        debtor.has_server_account = False
        debtor.balance = 0
        debtor.transfer_note_max_bytes = 0
        debtor.account_id = ""
        debtor.is_config_effectual = False


@atomic
def process_rejected_issuing_transfer_signal(
    *,
    coordinator_id: int,
    coordinator_request_id: int,
    status_code: str,
    total_locked_amount: int,
    debtor_id: int,
    creditor_id: int
) -> None:
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_finalized:
        if (
            status_code != SC_OK
            and rt.debtor_id == debtor_id
            and ROOT_CREDITOR_ID == creditor_id
        ):
            _finalize_running_transfer(
                rt,
                error_code=status_code,
                total_locked_amount=total_locked_amount,
            )
        else:  # pragma:  no cover
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_prepared_issuing_transfer_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_id: int,
    coordinator_request_id: int,
    locked_amount: int,
    recipient: str
) -> None:
    def dismiss_prepared_transfer():
        db.session.add(
            FinalizeTransferSignal(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=0,
                transfer_note_format="",
                transfer_note="",
            )
        )

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)

    the_signal_matches_the_transfer = (
        rt is not None
        and rt.debtor_id == debtor_id
        and ROOT_CREDITOR_ID == creditor_id
        and rt.recipient == recipient
        and rt.amount <= locked_amount
    )
    if the_signal_matches_the_transfer:
        assert rt is not None

        if not rt.is_finalized and rt.transfer_id is None:
            rt.transfer_id = transfer_id

        if rt.transfer_id == transfer_id:
            db.session.add(
                FinalizeTransferSignal(
                    debtor_id=rt.debtor_id,
                    creditor_id=ROOT_CREDITOR_ID,
                    transfer_id=transfer_id,
                    coordinator_id=coordinator_id,
                    coordinator_request_id=coordinator_request_id,
                    committed_amount=rt.amount,
                    transfer_note_format=rt.transfer_note_format,
                    transfer_note=rt.transfer_note,
                )
            )
            return

    dismiss_prepared_transfer()


@atomic
def process_finalized_issuing_transfer_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_id: int,
    coordinator_request_id: int,
    committed_amount: int,
    status_code: str,
    total_locked_amount: int
) -> None:
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)

    the_signal_matches_the_transfer = (
        rt is not None
        and rt.debtor_id == debtor_id
        and ROOT_CREDITOR_ID == creditor_id
        and rt.transfer_id == transfer_id
    )
    if the_signal_matches_the_transfer:
        assert rt is not None

        if status_code == SC_OK and committed_amount == rt.amount:
            _finalize_running_transfer(rt)
        elif status_code != SC_OK and committed_amount == 0:
            _finalize_running_transfer(
                rt,
                error_code=status_code,
                total_locked_amount=total_locked_amount,
            )
        else:  # pragma: no cover
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_account_update_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    creation_date: date,
    last_change_ts: datetime,
    last_change_seqnum: int,
    principal: int,
    interest_rate: float,
    last_config_ts: datetime,
    last_config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    account_id: str,
    transfer_note_max_bytes: int,
    ts: datetime,
    ttl: int
) -> None:
    if creditor_id != ROOT_CREDITOR_ID:  # pragma: no cover
        return

    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        _discard_orphaned_account(debtor_id, config_flags, negligible_amount)
        return

    if ts > debtor.account_last_heartbeat_ts:
        debtor.account_last_heartbeat_ts = min(ts, current_ts)

    prev_event = (
        debtor.account_creation_date,
        debtor.account_last_change_ts,
        Seqnum(debtor.account_last_change_seqnum),
    )
    this_event = (
        creation_date,
        last_change_ts,
        Seqnum(last_change_seqnum),
    )
    if this_event <= prev_event:
        return

    assert creation_date >= debtor.account_creation_date
    is_config_effectual = (
        last_config_ts == debtor.last_config_ts
        and last_config_seqnum == debtor.last_config_seqnum
        and config_flags == debtor.config_flags
        and config_data == debtor.config_data
        and abs(debtor.min_balance + negligible_amount)
        <= EPS * negligible_amount
    )

    debtor.is_config_effectual = is_config_effectual
    debtor.config_error = None if is_config_effectual else debtor.config_error
    debtor.has_server_account = True
    debtor.account_creation_date = creation_date
    debtor.account_last_change_ts = last_change_ts
    debtor.account_last_change_seqnum = last_change_seqnum
    debtor.account_id = account_id
    debtor.balance = principal
    debtor.transfer_note_max_bytes = transfer_note_max_bytes
    if is_config_effectual:
        debtor.debtor_info_iri = _get_debtor_info_iri_from_config_data(
            config_data
        )


@atomic
def save_document(
    *,
    debtor_id: int,
    content_type: str,
    content: bytes,
    max_saves_per_year: int = MAX_INT32
) -> Document:
    assert content_type is not None
    assert content is not None

    _throttle_document_saves(
        debtor_id, max_saves_per_year, datetime.now(tz=timezone.utc)
    )

    document = Document(
        debtor_id=debtor_id,
        content_type=content_type,
        content=content,
    )
    with db.retry_on_integrity_error():
        db.session.add(document)

    return document


@atomic
def get_document(debtor_id: int, document_id: int) -> Optional[Document]:
    return Document.query.filter_by(
        debtor_id=debtor_id, document_id=document_id
    ).one_or_none()


def _get_debtor_info_iri_from_config_data(config_data: str) -> Optional[str]:
    """Parse `config_data` and return `config_data['info']['iri']`."""

    def get_dict_key(d, key):
        return d.get(key) if isinstance(d, dict) else None

    try:
        config_data = json.loads(config_data)
    except json.JSONDecodeError:
        return None

    debtor_info = get_dict_key(config_data, "info")
    debtor_info_iri = get_dict_key(debtor_info, "iri")

    return debtor_info_iri if isinstance(debtor_info_iri, str) else None


def _throttle_debtor_actions(
    debtor_id: int, max_actions_per_month: int, current_ts: datetime
) -> Debtor:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        raise DebtorDoesNotExist()

    current_date = current_ts.date()
    number_of_elapsed_days = (
        current_date - debtor.actions_count_reset_date
    ).days
    if number_of_elapsed_days > 30:  # pragma: no cover
        debtor.actions_count = 0
        debtor.actions_count_reset_date = current_date

    if debtor.actions_count >= max_actions_per_month:
        raise TooManyManagementActions()

    debtor.actions_count += 1
    return debtor


def _throttle_document_saves(
    debtor_id: int, max_saves_per_year: int, current_ts: datetime
) -> Debtor:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        raise DebtorDoesNotExist()

    current_date = current_ts.date()
    number_of_elapsed_days = (
        current_date - debtor.documents_count_reset_date
    ).days
    if number_of_elapsed_days > 365:  # pragma: no cover
        debtor.documents_count = 0
        debtor.documents_count_reset_date = current_date

    if debtor.documents_count >= max_saves_per_year:
        raise TooManySavedDocuments()

    debtor.documents_count += 1
    return debtor


def _find_running_transfer(
    coordinator_id: int, coordinator_request_id: int
) -> Optional[RunningTransfer]:
    return RunningTransfer.query.filter_by(
        debtor_id=coordinator_id, coordinator_request_id=coordinator_request_id
    ).one_or_none()


def _insert_configure_account_signal(debtor: Debtor) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    debtor.last_config_ts = max(current_ts, debtor.last_config_ts)
    debtor.last_config_seqnum = increment_seqnum(debtor.last_config_seqnum)

    db.session.add(
        ConfigureAccountSignal(
            debtor_id=debtor.debtor_id,
            ts=debtor.last_config_ts,
            seqnum=debtor.last_config_seqnum,
            config_data=debtor.config_data,
            config_flags=debtor.config_flags,
            negligible_amount=-float(debtor.min_balance),
        )
    )


def _delete_debtor_transfers(debtor: Debtor) -> None:
    debtor.running_transfers_count = 0

    RunningTransfer.query.filter_by(debtor_id=debtor.debtor_id).delete(
        synchronize_session=False
    )


def _finalize_running_transfer(
    rt: RunningTransfer,
    error_code: str = None,
    total_locked_amount: int = None,
) -> None:
    if not rt.is_finalized:
        rt.finalized_at = datetime.now(tz=timezone.utc)
        rt.error_code = error_code
        rt.total_locked_amount = total_locked_amount


def _allow_update(
    obj, update_id_field_name: str, update_id: int, update: Dict[str, Any]
) -> Callable[[], None]:
    """Return a function that performs the update on `obj`.

    Raises `UpdateConflict` if the update is not allowed. Raises
    `AlreadyUpToDate` when the object is already up-to-date.

    """

    def has_changes():
        return any(
            [
                getattr(obj, field_name) != value
                for field_name, value in update.items()
            ]
        )

    def set_values():
        setattr(obj, update_id_field_name, update_id)
        for field_name, value in update.items():
            setattr(obj, field_name, value)
        return True

    latest_update_id = getattr(obj, update_id_field_name)
    if update_id == latest_update_id and not has_changes():
        raise AlreadyUpToDate()

    if update_id != latest_update_id + 1:
        raise UpdateConflict()

    return set_values


def _discard_orphaned_account(
    debtor_id: int, config_flags: int, negligible_amount: float
) -> None:
    scheduled_for_deletion_flag = Debtor.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT
    is_already_discarded = (
        config_flags & scheduled_for_deletion_flag
        and negligible_amount >= safely_huge_amount
    )

    if not is_already_discarded:
        db.session.add(
            ConfigureAccountSignal(
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                config_data="",
                config_flags=(
                    DEFAULT_CONFIG_FLAGS | scheduled_for_deletion_flag
                ),
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
            )
        )
