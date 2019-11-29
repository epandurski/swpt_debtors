from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .models import PendingTransfer
from .schemas import DebtorCreationRequestSchema, DebtorSchema, DebtorPolicyUpdateRequestSchema, \
    DebtorPolicySchema, TransferSchema, TransfersCollectionSchema, TransferCreationRequestSchema
from . import procedures

SPEC_DEBTOR_ID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'integer',
    },
}
SPEC_TRANSFER_UUID = {
    'in': 'path',
    'name': 'transferUuid',
    'required': True,
    'description': "The transfer's UUID",
    'schema': {
        'type': 'string',
    },
}
SPEC_LOCATION_HEADER = {
    'Location': {
        'description': 'The URI of the entry.',
        'schema': {
            'type': 'string',
            'format': 'uri',
        },
    },
}
SPEC_DEBTOR_DOES_NOT_EXIST = {
    'description': 'The debtor does not exist.',
}
SPEC_CONFLICTING_DEBTOR = {
    'description': 'A debtor with the same ID already exists.',
}
SPEC_CONFLICTING_POLICY = {
    'description': 'The new policy is in conflict with the old one.',
}
SPEC_TRANSFER_DOES_NOT_EXIST = {
    'description': 'The transfer entry does not exist.',
}
SPEC_CONFLICTING_TRANSFER = {
    'description': 'A different transfer entry with the same UUID already exists.',
}
SPEC_TOO_MANY_TRANSFERS = {
    'description': 'Too many pending transfers.',
}
SPEC_DUPLICATED_TRANSFER = {
    'description': 'The same transfer entry already exists.',
    'headers': SPEC_LOCATION_HEADER,
}

admin_api = Blueprint(
    'admin',
    __name__,
    url_prefix='/debtors',
    description="Create new debtors.",
)
public_api = Blueprint(
    'public',
    __name__,
    url_prefix='/debtors',
    description="Obtain public information about debtors.",
)
policy_api = Blueprint(
    'policy',
    __name__,
    url_prefix='/debtors',
    description="Change individual debtor's policies.",
)
transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/debtors',
    description="Make credit-issuing transfers.",
)


contextedDebtorSchema = DebtorSchema(context={'endpoint': 'public.Debtor'})
contextedDebtorPolicySchema = DebtorPolicySchema(context={'endpoint': 'policy.DebtorPolicy'})
contextedTransfersCollectionSchema = TransfersCollectionSchema(context={'endpoint': 'transfers.TransfersCollection'})
contextedTransferSchema = TransferSchema(context={'endpoint': 'transfers.Transfer'})


@admin_api.route('')
class DebtorCreator(MethodView):
    @admin_api.arguments(DebtorCreationRequestSchema)
    @admin_api.response(contextedDebtorSchema, code=201, headers=SPEC_LOCATION_HEADER)
    @admin_api.doc(responses={409: SPEC_CONFLICTING_DEBTOR})
    def post(self, debtor_info):
        """Try to create a new debtor."""

        debtor_id = debtor_info['debtor_id']
        try:
            debtor = procedures.create_new_debtor(debtor_id)
        except procedures.DebtorExistsError:
            abort(409)
        return debtor, {'Location': endpoints.build_url('debtor', debtorId=debtor_id)}


@public_api.route('/<i64:debtorId>', parameters=[SPEC_DEBTOR_ID], endpoint='Debtor')
class DebtorInfo(MethodView):
    @public_api.response(contextedDebtorSchema)
    @public_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return information about a debtor.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor or abort(404)


@policy_api.route('/<i64:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @policy_api.response(contextedDebtorPolicySchema)
    @policy_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return information about debtor's policy."""

        debtor = procedures.get_debtor(debtorId)
        return debtor or abort(404)

    @policy_api.arguments(DebtorPolicyUpdateRequestSchema)
    @policy_api.response(code=204)
    @policy_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST,
                               409: SPEC_CONFLICTING_POLICY})
    def patch(self, debtor_info, debtorId):
        """Update debtor's policy.

        This operation is **idempotent**!
        """

        # TODO: abort(409, message='fdfd', headers={'xxxyyy': 'zzz'})
        abort(409)
        abort(404)


@transfers_api.route('/<i64:debtorId>/transfers', parameters=[SPEC_DEBTOR_ID])
class TransfersCollection(MethodView):
    @transfers_api.response(contextedTransfersCollectionSchema)
    @transfers_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return all credit-issuing transfers for a given debtor."""

        return debtorId, [url_for('transfers.Transfer', debtorId=debtorId, transferUuid=uuid) for uuid in range(10)]

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(contextedTransferSchema, code=201, headers=SPEC_LOCATION_HEADER)
    @transfers_api.doc(responses={303: SPEC_DUPLICATED_TRANSFER,
                                  403: SPEC_TOO_MANY_TRANSFERS,
                                  404: SPEC_DEBTOR_DOES_NOT_EXIST,
                                  409: SPEC_CONFLICTING_TRANSFER})
    def post(self, transfer_request, debtorId):
        """Create a new credit-issuing transfer."""

        debtor = procedures.get_or_create_debtor(debtorId)
        transfer_uuid = transfer_request['transfer_uuid']
        location = url_for('transfers.Transfer', _external=True, debtorId=debtorId, transferUuid=transfer_uuid)
        recipient_url = urljoin(request.base_url, transfer_request['recipient'])
        try:
            # TODO: Write `transfer_request['recipient']` directly to the DB.
            transfer = procedures.create_pending_transfer(
                debtorId,
                transfer_uuid,
                endpoints.match_url('creditor', recipient_url)['creditorId'],
                transfer_request['amount'],
                transfer_request['transfer_info'],
            )
        except procedures.TransferExistsError:
            return redirect(location, code=303)
        except procedures.TransfersConflictError:
            abort(409)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:debtorId>/transfers/<transferUuid>', parameters=[SPEC_DEBTOR_ID, SPEC_TRANSFER_UUID])
class Transfer(MethodView):
    @transfers_api.response(contextedTransferSchema)
    @transfers_api.doc(responses={404: SPEC_TRANSFER_DOES_NOT_EXIST})
    def get(self, debtorId, transferUuid):
        """Return details about a credit-issuing transfer."""

        class Transfer:
            pass
        transfer = PendingTransfer.get_instance((debtorId, transferUuid))
        if transfer:
            return transfer
        abort(404)

    @transfers_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Purge a finalized credit-issuing transfer."""
