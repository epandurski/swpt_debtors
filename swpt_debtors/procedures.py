from datetime import datetime, date, timedelta, timezone
from random import randint
from uuid import UUID
from typing import TypeVar, Optional, Callable, List, Tuple
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import exc
from sqlalchemy.sql.expression import true
from swpt_lib.utils import Seqnum
from .extensions import db
from .lower_limits import LowerLimitSequence, TooLongLimitSequence
from .models import Debtor, Account, ChangeInterestRateSignal, FinalizeTransferSignal, \
    RunningTransfer, PrepareTransferSignal, ConfigureAccountSignal, \
    NodeConfig, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID, \
    SC_UNEXPECTED_ERROR, SC_CANCELED_BY_THE_SENDER, SC_OK

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_SECOND = timedelta(seconds=1)


class MisconfiguredNode(Exception):
    """The node is misconfigured."""


class InvalidDebtor(Exception):
    """The node is not responsible for this debtor."""


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


class ForbiddenTransferCancellation(Exception):
    """The transfer can not be canceled."""


class TooManyManagementActions(Exception):
    """Too many management actions per month by a debtor."""


class ConflictingPolicy(Exception):
    """The new debtor policy conflicts with the old one."""

    def __init__(self, message: str):
        self.message = message


@atomic
def configure_node(min_debtor_id: int, max_debtor_id: int) -> None:
    assert MIN_INT64 <= min_debtor_id <= MAX_INT64
    assert MIN_INT64 <= max_debtor_id <= MAX_INT64
    assert min_debtor_id <= max_debtor_id

    agent_config = NodeConfig.query.with_for_update().one_or_none()

    if agent_config:
        agent_config.min_debtor_id = min_debtor_id
        agent_config.max_debtor_id = max_debtor_id
    else:  # pragma: no cover
        with db.retry_on_integrity_error():
            db.session.add(NodeConfig(
                min_debtor_id=min_debtor_id,
                max_debtor_id=max_debtor_id,
            ))


@atomic
def generate_new_debtor_id() -> int:
    node_config = _get_node_config()
    return randint(node_config.min_debtor_id, node_config.max_debtor_id)


@atomic
def get_debtor_ids(start_from: int, count: int = 1) -> Tuple[List[int], Optional[int]]:
    query = db.session.\
        query(Debtor.debtor_id).\
        filter(Debtor.debtor_id >= start_from).\
        filter(Debtor.status_flags.op('&')(Debtor.STATUS_IS_ACTIVATED_FLAG) != 0).\
        order_by(Debtor.debtor_id).\
        limit(count)
    debtor_ids = [t[0] for t in query.all()]

    if len(debtor_ids) > 0:
        next_debtor_id = debtor_ids[-1] + 1
    else:
        next_debtor_id = _get_node_config().max_debtor_id + 1

    if next_debtor_id > MAX_INT64 or next_debtor_id <= start_from:
        next_debtor_id = None

    return debtor_ids, next_debtor_id


@atomic
def reserve_debtor(debtor_id, verify_correctness=True) -> Debtor:
    if verify_correctness and not _is_correct_debtor_id(debtor_id):
        raise InvalidDebtor()

    debtor = Debtor(debtor_id=debtor_id)
    db.session.add(debtor)
    try:
        db.session.flush()
    except IntegrityError:
        raise DebtorExists() from None

    return debtor


@atomic
def activate_debtor(debtor_id: int, reservation_id: int) -> Debtor:
    debtor = get_debtor(debtor_id, lock=True)
    if debtor is None:
        raise InvalidReservationId()

    if not debtor.is_activated:
        if reservation_id != debtor.reservation_id or debtor.is_deactivated:
            raise InvalidReservationId()
        debtor.activate()
        _insert_configure_account_signal(debtor_id)

    return debtor


@atomic
def deactivate_debtor(debtor_id: int, deleted_account: bool = False) -> None:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor:
        debtor.deactivate()
        _delete_debtor_transfers(debtor)


@atomic
def get_debtor(debtor_id: int, lock: bool = False) -> Optional[Debtor]:
    query = Debtor.query.filter_by(debtor_id=debtor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def get_active_debtor(debtor_id: int, lock: bool = False) -> Optional[Debtor]:
    debtor = get_debtor(debtor_id, lock=lock)
    if debtor and debtor.is_activated and not debtor.is_deactivated:
        return debtor


@atomic
def update_debtor_balance(debtor_id: int, balance: int) -> None:
    debtor = get_debtor(debtor_id, lock=True)
    if debtor is None:
        if not _is_correct_debtor_id(debtor_id):  # pragma: no cover
            return

        # Normally, this should never happen. If it does happen,
        # though, we create a new deactivated debtor, not allowing the
        # debtor ID to be used.
        debtor = Debtor(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(debtor)
        debtor.activate()
        debtor.deactivate()

    if debtor.balance != balance:
        debtor.balance = balance


@atomic
def update_debtor(
        debtor_id: int,
        interest_rate_target: float,
        new_interest_rate_limits: LowerLimitSequence,
        new_balance_limits: LowerLimitSequence,
        debtor_info_iri: Optional[str],
        debtor_info_content_type: Optional[str],
        debtor_info_sha256: Optional[bytes]) -> Debtor:

    current_ts = datetime.now(tz=timezone.utc)
    debtor = _throttle_debtor_actions(debtor_id, current_ts)
    date_week_ago = (current_ts - timedelta(days=7)).date()

    interest_rate_lower_limits = debtor.interest_rate_lower_limits
    interest_rate_lower_limits = interest_rate_lower_limits.current_limits(date_week_ago)
    try:
        interest_rate_lower_limits.add_limits(new_interest_rate_limits)
    except TooLongLimitSequence:
        raise ConflictingPolicy('There are too many interest rate limits.')

    balance_lower_limits = debtor.balance_lower_limits
    balance_lower_limits = balance_lower_limits.current_limits(date_week_ago)
    try:
        balance_lower_limits.add_limits(new_balance_limits)
    except TooLongLimitSequence:
        raise ConflictingPolicy('There are too many balance limits.')

    debtor.interest_rate_target = interest_rate_target
    debtor.interest_rate_lower_limits = interest_rate_lower_limits
    debtor.balance_lower_limits = balance_lower_limits
    debtor.debtor_info_iri = debtor_info_iri
    debtor.debtor_info_sha256 = debtor_info_sha256
    debtor.debtor_info_content_type = debtor_info_content_type

    return debtor


@atomic
def get_debtor_transfer_uuids(debtor_id: int) -> List[UUID]:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        raise DebtorDoesNotExist()

    rows = db.session.\
        query(RunningTransfer.transfer_uuid).\
        filter_by(debtor_id=debtor_id).\
        all()

    return [uuid for (uuid,) in rows]


@atomic
def get_running_transfer(debtor_id: int, transfer_uuid: UUID, lock=False) -> Optional[RunningTransfer]:
    query = RunningTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def cancel_running_transfer(debtor_id: int, transfer_uuid: UUID) -> RunningTransfer:
    rt = get_running_transfer(debtor_id, transfer_uuid, lock=True)
    if rt is None:
        raise TransferDoesNotExist()

    if rt.is_settled:
        raise ForbiddenTransferCancellation()

    _finalize_running_transfer(rt, error_code=SC_CANCELED_BY_THE_SENDER)
    return rt


@atomic
def delete_running_transfer(debtor_id: int, transfer_uuid: UUID) -> None:
    number_of_deleted_rows = RunningTransfer.query.\
        filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).\
        delete(synchronize_session=False)

    if number_of_deleted_rows == 0:
        raise TransferDoesNotExist()

    assert number_of_deleted_rows == 1
    Debtor.query.\
        filter_by(debtor_id=debtor_id).\
        update({Debtor.running_transfers_count: Debtor.running_transfers_count - 1}, synchronize_session=False)


@atomic
def initiate_running_transfer(
        debtor_id: int,
        transfer_uuid: UUID,
        recipient_uri: str,
        recipient: str,
        amount: int,
        transfer_note_format: str,
        transfer_note: str) -> RunningTransfer:

    current_ts = datetime.now(tz=timezone.utc)
    transfer_data = {
        'amount': amount,
        'recipient_uri': recipient_uri,
        'recipient': recipient,
        'transfer_note_format': transfer_note_format,
        'transfer_note': transfer_note,
    }

    rt = get_running_transfer(debtor_id, transfer_uuid)
    if rt:
        if any(getattr(rt, attr) != value for attr, value in transfer_data.items()):
            raise TransfersConflict()
        raise TransferExists()

    debtor = _throttle_debtor_actions(debtor_id, current_ts)
    debtor.running_transfers_count += 1
    if debtor.running_transfers_count > current_app.config['APP_MAX_TRANSFERS_PER_MONTH']:
        raise TransfersConflict()

    new_running_transfer = RunningTransfer(
        debtor_id=debtor_id,
        transfer_uuid=transfer_uuid,
        **transfer_data,
    )
    with db.retry_on_integrity_error():
        db.session.add(new_running_transfer)

    db.session.add(PrepareTransferSignal(
        debtor_id=debtor_id,
        coordinator_request_id=new_running_transfer.coordinator_request_id,
        amount=amount,
        creditor_id=ROOT_CREDITOR_ID,
        recipient=recipient,
        min_account_balance=debtor.min_account_balance,
    ))

    return new_running_transfer


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    account = Account.lock_instance((debtor_id, creditor_id))
    if account and account.creation_date == creation_date:
        db.session.delete(account)


@atomic
def process_rejected_issuing_transfer_signal(
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        debtor_id: int,
        creditor_id: int) -> None:

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_finalized:
        if status_code != SC_OK and rt.debtor_id == debtor_id and ROOT_CREDITOR_ID == creditor_id:
            _finalize_running_transfer(rt, error_code=status_code, total_locked_amount=total_locked_amount)
        else:  # pragma:  no cover
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_prepared_issuing_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        recipient: str) -> None:

    def dismiss_prepared_transfer():
        db.session.add(FinalizeTransferSignal(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_id=transfer_id,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            committed_amount=0,
            transfer_note_format='',
            transfer_note='',
        ))

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
            db.session.add(FinalizeTransferSignal(
                debtor_id=rt.debtor_id,
                creditor_id=ROOT_CREDITOR_ID,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=rt.amount,
                transfer_note_format=rt.transfer_note_format,
                transfer_note=rt.transfer_note,
            ))
            return

    dismiss_prepared_transfer()


@atomic
def process_finalized_issuing_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient: str,
        committed_amount: int,
        status_code: str,
        total_locked_amount: int) -> None:

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)

    the_signal_matches_the_transfer = (
        rt is not None
        and rt.debtor_id == debtor_id
        and ROOT_CREDITOR_ID == creditor_id
        and rt.transfer_id == transfer_id
    )
    if the_signal_matches_the_transfer:
        assert rt is not None

        if status_code == SC_OK and committed_amount == rt.amount and recipient == rt.recipient:
            _finalize_running_transfer(rt)
        elif status_code != SC_OK and committed_amount == 0 and recipient == rt.recipient:
            _finalize_running_transfer(rt, error_code=status_code, total_locked_amount=total_locked_amount)
        else:  # pragma: no cover
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_interest_rate_change_ts: datetime,
        creation_date: date,
        negligible_amount: float,
        config_flags: int,
        status_flags: int,
        ts: datetime,
        ttl: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= last_change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert -100 < interest_rate <= 100.0
    assert negligible_amount >= 0.0
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert MIN_INT32 <= status_flags <= MAX_INT32
    assert ttl > 0

    current_ts = datetime.now(tz=timezone.utc)
    ts = min(ts, current_ts)
    if (current_ts - ts).total_seconds() > ttl:
        return

    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
        if ts > account.last_heartbeat_ts:
            account.last_heartbeat_ts = ts
        prev_event = (account.creation_date, account.last_change_ts, Seqnum(account.last_change_seqnum))
        this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
        if this_event <= prev_event:
            return
        account.last_change_seqnum = last_change_seqnum
        account.last_change_ts = last_change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_interest_rate_change_ts = last_interest_rate_change_ts
        account.creation_date = creation_date
        account.negligible_amount = negligible_amount
        account.config_flags = config_flags
        account.status_flags = status_flags
    else:
        account = Account(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            last_change_seqnum=last_change_seqnum,
            last_change_ts=last_change_ts,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_interest_rate_change_ts=last_interest_rate_change_ts,
            creation_date=creation_date,
            negligible_amount=negligible_amount,
            config_flags=config_flags,
            status_flags=status_flags,
            last_heartbeat_ts=ts,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    if account.creditor_id == ROOT_CREDITOR_ID:
        balance = MIN_INT64 if account.is_overflown else account.principal
        update_debtor_balance(debtor_id, balance)
    elif not account.status_flags & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        cutoff_ts = current_ts - Account.get_interest_rate_change_min_interval()
        debtor = Debtor.get_instance(debtor_id)
        if debtor and account.last_interest_rate_change_ts < cutoff_ts:
            account.is_muted = True
            account.last_maintenance_request_ts = current_ts
            insert_change_interest_rate_signal(debtor_id, creditor_id, debtor.interest_rate, current_ts)


@atomic
def process_account_maintenance_signal(debtor_id: int, creditor_id: int, request_ts: datetime) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    Account.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        filter(Account.is_muted == true()).\
        filter(Account.last_maintenance_request_ts <= request_ts + TD_SECOND).\
        update({Account.is_muted: False}, synchronize_session=False)


@atomic
def insert_change_interest_rate_signal(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        request_ts: datetime) -> None:

    db.session.add(ChangeInterestRateSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=interest_rate,
        request_ts=request_ts,
    ))


def _throttle_debtor_actions(debtor_id: int, current_ts: datetime) -> Debtor:
    debtor = get_active_debtor(debtor_id, lock=True)
    if debtor is None:
        raise DebtorDoesNotExist()

    current_date = current_ts.date()
    number_of_elapsed_days = (current_date - debtor.actions_count_reset_date).days
    if number_of_elapsed_days > 30:  # pragma: no cover
        debtor.actions_count = 0
        debtor.actions_count_reset_date = current_date

    if debtor.actions_count >= current_app.config['APP_MAX_TRANSFERS_PER_MONTH']:
        raise TooManyManagementActions()

    debtor.actions_count += 1
    return debtor


def _find_running_transfer(coordinator_id: int, coordinator_request_id: int) -> Optional[RunningTransfer]:
    return RunningTransfer.query.\
        filter_by(debtor_id=coordinator_id, coordinator_request_id=coordinator_request_id).\
        one_or_none()


def _insert_configure_account_signal(debtor_id: int) -> None:
    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        ts=datetime.now(tz=timezone.utc),
    ))


def _get_node_config() -> NodeConfig:
    try:
        return NodeConfig.query.one()
    except exc.NoResultFound:  # pragma: no cover
        raise MisconfiguredNode() from None


def _is_correct_debtor_id(debtor_id: int) -> bool:
    try:
        config = _get_node_config()
    except MisconfiguredNode:  # pragma: no cover
        return False

    if not config.min_debtor_id <= debtor_id <= config.max_debtor_id:
        return False

    return True


def _delete_debtor_transfers(debtor: Debtor) -> None:
    debtor.running_transfers_count = 0

    RunningTransfer.query.\
        filter_by(debtor_id=debtor.debtor_id).\
        delete(synchronize_session=False)


def _finalize_running_transfer(rt: RunningTransfer, error_code: str = None, total_locked_amount: int = None) -> None:
    if not rt.is_finalized:
        rt.finalized_at = datetime.now(tz=timezone.utc)
        rt.error_code = error_code
        rt.total_locked_amount = total_locked_amount
