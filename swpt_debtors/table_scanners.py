from typing import NamedTuple, Dict, List
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from .extensions import db
from .models import Debtor, RunningTransfer, Account, PurgeDeletedAccountSignal


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class RunningTransfersCollector(TableScanner):
    table = RunningTransfer.__table__
    columns = [RunningTransfer.debtor_id, RunningTransfer.transfer_uuid, RunningTransfer.finalized_at_ts]

    def __init__(self, days):
        super().__init__()
        self.days = days
        self.pk = tuple_(self.table.c.debtor_id, self.table.c.transfer_uuid)

    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - timedelta(days=self.days)
        pks_to_delete = [(row[0], row[1]) for row in rows if row[2] < cutoff_ts]
        if pks_to_delete:
            db.engine.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class AccountsScanner(TableScanner):
    table = Account.__table__
    c_debtor_id = table.c.debtor_id
    c_creditor_id = table.c.creditor_id
    c_change_ts = table.c.change_ts
    c_status = table.c.status
    old_interest_rate = CachedInterestRate(0.0, datetime(1900, 1, 1))
    pk = tuple_(Account.debtor_id, Account.creditor_id)

    def __init__(self, days: float):
        super().__init__()
        self.days = days
        self.debtor_interest_rates: Dict[int, CachedInterestRate] = {}

    def _get_debtor_interest_rates(self, debtor_ids: List[int]) -> List[float]:
        current_ts = datetime.now(tz=timezone.utc)
        cutoff_ts = current_ts - timedelta(days=self.days)
        rates = self.debtor_interest_rates
        old_rate = self.old_interest_rate
        old_rate_debtor_ids = [x for x in debtor_ids if rates.get(x, old_rate).timestamp < cutoff_ts]
        if old_rate_debtor_ids:
            for debtor in Debtor.query.filter(Debtor.debtor_id.in_(old_rate_debtor_ids)):
                rates[debtor.debtor_id] = CachedInterestRate(debtor.interest_rate, current_ts)
        return [rates.get(x, old_rate).interest_rate for x in debtor_ids]

    def _check_interest_rate(self, rows):
        pass

    def _check_accumulated_interest(self, rows):
        pass

    def _check_negative_balance(self, rows):
        pass

    def _check_if_deleted(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        cutoff_ts = current_ts  # TODO: Subtract something.
        pks_to_delete = []
        for row in rows:
            is_deleted = row[self.c_status] & Account.STATUS_DELETED_FLAG
            if is_deleted & row[self.c_change_ts] < cutoff_ts:
                debtor_id = row[self.c_debtor_id]
                creditor_id = row[self.c_creditor_id]
                db.session.add(PurgeDeletedAccountSignal(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    if_deleted_before=cutoff_ts,
                ))
                pks_to_delete.append((debtor_id, creditor_id))
        if pks_to_delete:
            Account.query.filter(self.pk.in_(pks_to_delete)).delete(synchronize_session=False)

    def process_rows(self, rows):
        self._check_interest_rate(rows)
        self._check_accumulated_interest(rows)
        self._check_negative_balance(rows)
        self._check_if_deleted(rows)
