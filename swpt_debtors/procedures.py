from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple
from sqlalchemy.exc import IntegrityError
from .extensions import db
from .models import Debtor, Account, ChangeInterestRateSignal, LowerLimitSequence, \
    InitiatedTransfer, RunningTransfer, PrepareTransferSignal, increment_seqnum, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_ZERO = timedelta(seconds=0)
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND


class DebtorExistsError(Exception):
    """The same debtor record already exists."""


class TransferExistsError(Exception):
    """The same initiated transfer record already exists."""

    def __init__(self, transfer: InitiatedTransfer):
        self.transfer = transfer


class TransfersConflictError(Exception):
    """A different transfer with the same UUID already exists."""


@atomic
def get_debtor(debtor_id: int) -> Optional[Debtor]:
    return Debtor.get_instance(debtor_id)


@atomic
def update_debtor_policy(
        debtor_id: int,
        interest_rate_target: float,
        new_interest_rate_limits: LowerLimitSequence,
        new_balance_limits: LowerLimitSequence):
    # TODO: This is probably not at all the function we need.

    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        # TODO: define own exception type.
        raise Exception()

    interest_rate_lower_limits = debtor.interest_rate_lower_limits
    for l in new_interest_rate_limits:
        interest_rate_lower_limits.add_limit(l)
    balance_lower_limits = debtor.balance_lower_limits
    for l in new_balance_limits:
        balance_lower_limits.add_limit(l)
    debtor.interest_rate_target = interest_rate_target
    debtor.interest_rate_lower_limits = interest_rate_lower_limits
    debtor.balance_lower_limits = balance_lower_limits


def _is_later_event(event: Tuple[int, datetime], other_event: Tuple[Optional[int], Optional[datetime]]) -> bool:
    seqnum, ts = event
    other_seqnum, other_ts = other_event
    if other_ts:
        advance = ts - other_ts
    else:
        advance = TD_ZERO
    return advance >= TD_MINUS_SECOND and (
        advance > TD_SECOND
        or other_seqnum is None
        or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000
    )


def _insert_change_interest_rate_signal(account: Account, interest_rate: Optional[float]) -> None:
    if interest_rate is not None:
        current_ts = datetime.now(tz=timezone.utc)
        account.interest_rate_last_change_seqnum = increment_seqnum(account.interest_rate_last_change_seqnum)
        account.interest_rate_last_change_ts = max(account.interest_rate_last_change_ts, current_ts)
        db.session.add(ChangeInterestRateSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            change_seqnum=account.interest_rate_last_change_seqnum,
            change_ts=account.interest_rate_last_change_ts,
            interest_rate=interest_rate,
        ))


def _compare_initiated_transfers(first: InitiatedTransfer, second: InitiatedTransfer) -> bool:
    return all([
        first.debtor_id == second.debtor_id,
        first.transfer_uuid == second.transfer_uuid,
        first.recipient_uri == second.recipient_uri,
        first.amount == second.amount,
        first.transfer_info == second.transfer_info,
    ])


@atomic
def create_new_debtor(debtor_id: int) -> Optional[Debtor]:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor(debtor_id=debtor_id)
    db.session.add(debtor)
    try:
        db.session.flush()
    except IntegrityError:
        raise DebtorExistsError(debtor_id)
    return debtor


@atomic
def get_or_create_debtor(debtor_id: int) -> Debtor:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        debtor = Debtor(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(debtor)
    return debtor


@atomic
def initiate_transfer(debtor_id: int,
                      transfer_uuid: UUID,
                      recipient_creditor_id: int,
                      recipient_uri: str,
                      amount: int,
                      transfer_info: dict) -> InitiatedTransfer:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert recipient_creditor_id is None or MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert 0 < amount <= MAX_INT64

    # Create an `InitiatedTransfer` record.
    new_initiated_transfer = InitiatedTransfer(
        debtor_id=debtor_id,
        transfer_uuid=transfer_uuid,
        recipient_uri=recipient_uri,
        amount=amount,
        transfer_info=transfer_info,
        finalized_at_ts=datetime.now(tz=timezone.utc) if recipient_creditor_id is None else None,
    )
    existing_initiated_transfer = InitiatedTransfer.get_instance((debtor_id, transfer_uuid))
    if existing_initiated_transfer:
        if _compare_initiated_transfers(new_initiated_transfer, existing_initiated_transfer):
            raise TransferExistsError(existing_initiated_transfer)
        else:
            raise TransfersConflictError
    with db.retry_on_integrity_error():
        db.session.add(new_initiated_transfer)

    if recipient_creditor_id is not None:
        # Create an `RunningTransfer` record.
        running_transfer = RunningTransfer(
            debtor_id=debtor_id,
            transfer_uuid=transfer_uuid,
            recipient_creditor_id=recipient_creditor_id,
            amount=amount,
            transfer_info=transfer_info,
        )
        db.session.add(running_transfer)
        try:
            db.session.flush()
        except IntegrityError:
            raise TransfersConflictError

        # Send a `prepare_transfer` message.
        db.session.add(PrepareTransferSignal(
            debtor_id=debtor_id,
            coordinator_request_id=running_transfer.issuing_coordinator_request_id,
            min_amount=amount,
            max_amount=amount,
            sender_creditor_id=ROOT_CREDITOR_ID,
            recipient_creditor_id=recipient_creditor_id,
        ))

    return new_initiated_transfer


@atomic
def process_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_seqnum: int,
        change_ts: datetime,
        principal: int,
        interest: float,
        interest_rate: float,
        last_outgoing_transfer_date: date,
        status: int) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert -100 < interest_rate <= 100.0
    assert MIN_INT16 <= status <= MAX_INT16

    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
        this_event = (change_seqnum, change_ts)
        prev_event = (account.change_seqnum, account.change_ts)
        if not _is_later_event(this_event, prev_event):
            return
        account.change_seqnum = change_seqnum
        account.change_ts = change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_outgoing_transfer_date = last_outgoing_transfer_date
        account.status = status
    else:
        account = Account(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            change_seqnum=change_seqnum,
            change_ts=change_ts,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_outgoing_transfer_date=last_outgoing_transfer_date,
            status=status,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    # When the account does not have an interest rate set yet, we must
    # immediately calculate the interest rate currently applied by the
    # debtor, and send a `ChangeInterestRateSignal`.
    if not account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        debtor = Debtor.get_instance(debtor_id)
        if debtor:
            _insert_change_interest_rate_signal(account, debtor.interest_rate)
