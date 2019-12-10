import pytest
from uuid import UUID
from datetime import datetime, date, timedelta
from swpt_debtors import __version__
from swpt_debtors.models import Account, ChangeInterestRateSignal, InitiatedTransfer, RunningTransfer, \
    PrepareTransferSignal, FinalizePreparedTransferSignal, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, ROOT_CREDITOR_ID
from swpt_debtors import procedures as p
from swpt_debtors.lower_limits import LowerLimit

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


@pytest.fixture
def debtor():
    return p.get_or_create_debtor(D_ID)


def test_version(db_session):
    assert __version__


def test_is_later_event(current_ts):
    assert p._is_later_event((1, current_ts), (None, None))


def test_get_or_create_debtor(db_session):
    debtor = p.get_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    assert not debtor.is_active
    assert debtor.deactivated_at_date is None
    debtor = p.get_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    assert not debtor.is_active
    assert debtor.deactivated_at_date is None


def test_terminate_debtor(db_session, debtor):
    p.terminate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert not debtor.is_active
    assert debtor.deactivated_at_date is not None

    p.terminate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert not debtor.is_active
    assert debtor.deactivated_at_date is not None

    p.terminate_debtor(1234567890)
    assert p.get_debtor(1234567890) is None


def test_process_account_change_signal(db_session, debtor):
    change_seqnum = 1
    change_ts = datetime.fromisoformat('2019-10-01T00:00:00+00:00')
    last_outgoing_transfer_date = date.fromisoformat('2019-10-01')
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        status=0,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum
    assert a.change_ts == change_ts
    assert a.principal == 1000
    assert a.interest == 12.5
    assert a.interest_rate == -0.5
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date
    assert a.status == 0
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1
    assert cirs[0].debtor_id == D_ID
    assert cirs[0].creditor_id == C_ID

    # Older message
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum - 1,
        change_ts=change_ts,
        principal=1001,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        status=0,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.principal == 1000
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1

    # Newer message
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum + 1,
        change_ts=change_ts + timedelta(seconds=5),
        principal=1001,
        interest=12.6,
        interest_rate=-0.6,
        last_outgoing_transfer_date=last_outgoing_transfer_date + timedelta(days=1),
        status=Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum + 1
    assert a.change_ts == change_ts + timedelta(seconds=5)
    assert a.principal == 1001
    assert a.interest == 12.6
    assert a.interest_rate == -0.6
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date + timedelta(days=1)
    assert a.status == Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1


def test_process_root_account_change_signal(db_session, debtor):
    change_seqnum = 1
    change_ts = datetime.fromisoformat('2019-10-01T00:00:00+00:00')
    last_outgoing_transfer_date = date.fromisoformat('2019-10-01')
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=-9999,
        interest=0,
        interest_rate=0.0,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        status=0,
    )
    d = p.get_debtor(D_ID)
    assert d.balance == -9999
    assert d.balance_ts == change_ts


def test_interest_rate_absolute_limits(db_session, debtor):
    debtor.interest_rate_target = -100.0
    assert debtor.interest_rate == INTEREST_RATE_FLOOR
    debtor.interest_rate_target = 1e100
    assert debtor.interest_rate == INTEREST_RATE_CEIL


def test_update_debtor_policy(db_session, debtor, current_ts):
    date_years_ago = (current_ts - timedelta(days=5000)).date()
    with pytest.raises(p.DebtorDoesNotExistError):
        p.update_debtor_policy(1234567890, 6.66, [], [])

    p.update_debtor_policy(D_ID, 6.66, [LowerLimit(0.0, date_years_ago)], [LowerLimit(-1000, date_years_ago)])
    debtor = p.get_debtor(D_ID)
    assert debtor.is_active
    assert debtor.interest_rate_target == 6.66
    assert len(debtor.interest_rate_lower_limits) == 1
    assert debtor.interest_rate_lower_limits[0] == LowerLimit(0.0, date_years_ago)
    assert len(debtor.balance_lower_limits) == 1
    assert debtor.balance_lower_limits[0] == LowerLimit(-1000, date_years_ago)

    p.update_debtor_policy(D_ID, None, [], [])
    debtor = p.get_debtor(D_ID)
    assert debtor.interest_rate_target == 6.66
    assert len(debtor.interest_rate_lower_limits) == 0
    assert len(debtor.balance_lower_limits) == 0

    with pytest.raises(p.ConflictingPolicyError):
        p.update_debtor_policy(D_ID, None, 11 * [LowerLimit(0.0, current_ts.date())], [])
    with pytest.raises(p.ConflictingPolicyError):
        p.update_debtor_policy(D_ID, None, [], 11 * [LowerLimit(-1000, current_ts.date())])


def test_initiated_transfers(db_session, debtor):
    db_session.add(InitiatedTransfer(
        debtor_id=D_ID,
        transfer_uuid=TEST_UUID,
        recipient_uri=RECIPIENT_URI,
        amount=1001,
    ))
    db_session.commit()
    with pytest.raises(p.DebtorDoesNotExistError):
        p.get_debtor_transfer_uuids(1234567890)
    uuids = p.get_debtor_transfer_uuids(D_ID)
    assert uuids == [TEST_UUID]

    assert p.get_initiated_transfer(1234567890, TEST_UUID) is None
    t = p.get_initiated_transfer(D_ID, TEST_UUID)
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.amount == 1001
    assert t.recipient_uri == RECIPIENT_URI

    p.delete_initiated_transfer(D_ID, TEST_UUID)
    assert p.get_initiated_transfer(D_ID, TEST_UUID) is None


def test_create_new_debtor(db_session, debtor):
    with pytest.raises(p.DebtorExistsError):
        p.create_new_debtor(D_ID)
    debtor = p.create_new_debtor(1234567890)
    assert debtor.debtor_id == 1234567890


def test_initiate_transfer(db_session, debtor):
    assert len(RunningTransfer.query.all()) == 0
    assert len(InitiatedTransfer.query.all()) == 0
    assert p.get_debtor_transfer_uuids(D_ID) == []
    t = p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1000, {'note': 'test'})
    debtor = p.get_debtor(D_ID)
    assert debtor.is_active
    assert len(InitiatedTransfer.query.all()) == 1
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.recipient_uri == RECIPIENT_URI
    assert t.amount == 1000
    assert t.transfer_info == {'note': 'test'}
    assert not t.is_finalized
    running_transfers = RunningTransfer.query.all()
    assert len(running_transfers) == 1
    rt = running_transfers[0]
    assert rt.debtor_id == D_ID
    assert rt.transfer_uuid == TEST_UUID
    assert rt.recipient_creditor_id == C_ID
    assert rt.amount == 1000
    assert rt.transfer_info == {'note': 'test'}
    assert not t.is_finalized
    with pytest.raises(p.TransferExistsError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1000, {'note': 'test'})
    with pytest.raises(p.TransfersConflictError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1001, {'note': 'test'})
    with pytest.raises(p.DebtorDoesNotExistError):
        p.initiate_transfer(1234567890, TEST_UUID, C_ID, RECIPIENT_URI, 1001, {'note': 'test'})
    assert len(p.get_debtor_transfer_uuids(D_ID)) == 1
    t2 = p.initiate_transfer(D_ID, TEST_UUID2, None, RECIPIENT_URI, 50, {})
    assert t2.is_finalized
    assert len(RunningTransfer.query.all()) == 1

    p.delete_initiated_transfer(D_ID, TEST_UUID)
    assert len(RunningTransfer.query.all()) == 1
    with pytest.raises(p.TransfersConflictError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1000, {'note': 'test'})


def test_successful_transfer(db_session, debtor):
    assert len(PrepareTransferSignal.query.all()) == 0
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1000, {'note': 'test'})
    pts_list = PrepareTransferSignal.query.all()
    assert len(pts_list) == 1
    pts = pts_list[0]
    assert pts.debtor_id == D_ID
    assert pts.coordinator_request_id is not None
    assert pts.min_amount == pts.max_amount == 1000
    assert pts.sender_creditor_id == ROOT_CREDITOR_ID
    assert pts.recipient_creditor_id == C_ID
    assert pts.minimum_account_balance == debtor.minimum_account_balance

    p.process_prepared_payment_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient_creditor_id=C_ID,
        sender_locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=pts.coordinator_request_id,
    )
    assert len(PrepareTransferSignal.query.all()) == 1
    fpts_list = FinalizePreparedTransferSignal.query.all()
    assert len(fpts_list) == 1
    fpts = fpts_list[0]
    assert fpts.debtor_id == D_ID
    assert fpts.sender_creditor_id == ROOT_CREDITOR_ID
    assert fpts.transfer_id is not None
    assert fpts.committed_amount == 1000
    assert fpts.transfer_info == {'note': 'test'}

    rt_list = RunningTransfer.query.all()
    assert len(rt_list) == 1
    rt = rt_list[0]
    assert rt.is_finalized
    assert rt.issuing_transfer_id is not None
    it_list = InitiatedTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert it.is_successful

    p.process_prepared_payment_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient_creditor_id=C_ID,
        sender_locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=pts.coordinator_request_id,
    )

    rt_list == RunningTransfer.query.all()
    assert len(rt_list) == 1 and rt_list[0].is_finalized
    it_list == InitiatedTransfer.query.all()
    assert len(it_list) == 1 and it_list[0].is_finalized


def test_failed_transfer(db_session, debtor):
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, RECIPIENT_URI, 1000, {'note': 'test'})
    pts = PrepareTransferSignal.query.all()[0]
    p.process_rejected_payment_transfer_signal(D_ID, pts.coordinator_request_id, details={
        'error_code': 'TEST',
        'message': 'A testing error.',
    })
    assert len(FinalizePreparedTransferSignal.query.all()) == 0

    rt_list = RunningTransfer.query.all()
    assert len(rt_list) == 1
    rt = rt_list[0]
    assert rt.is_finalized
    assert rt.issuing_transfer_id is None
    it_list = InitiatedTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert not it.is_successful

    p.process_rejected_payment_transfer_signal(D_ID, pts.coordinator_request_id, details={
        'error_code': 'TEST',
        'message': 'A testing error.',
    })
    rt_list == RunningTransfer.query.all()
    assert len(rt_list) == 1 and rt_list[0].is_finalized
    it_list == InitiatedTransfer.query.all()
    assert len(it_list) == 1 and it_list[0].is_finalized
