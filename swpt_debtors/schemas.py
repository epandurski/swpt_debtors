from copy import copy
from base64 import b16encode
from marshmallow import Schema, fields, validate, pre_dump, post_dump, post_load, \
    validates, missing, ValidationError
from flask import url_for, current_app
from swpt_lib.utils import i64_to_u64
from .lower_limits import LowerLimit
from .models import INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64, MAX_UINT64, \
    TRANSFER_NOTE_MAX_BYTES, SC_INSUFFICIENT_AVAILABLE_AMOUNT, Debtor, RunningTransfer

URI_DESCRIPTION = '\
The URI of this object. Can be a relative URI.'

PAGE_NEXT_DESCRIPTION = '\
An URI of another `{type}` object which contains more items. When \
there are no remaining items, this field will not be present. If this field \
is present, there might be remaining items, even when the `items` array is \
empty. This can be a relative URI.'

ESTABLISHED_LIMITS_NOTE = '\
**Note:** Established limits can not be removed. They will continue \
to be enforced until the specified expiration date. Therefore, when \
the policy is being updated, this field should contain only the \
additional limits that have to be added to the existing ones. This can \
be an empty array.'

TRANSFER_NOTE_FORMAT_REGEX = r'^[0-9A-Za-z.-]{0,8}$'

TRANSFER_NOTE_FORMAT_DESCRIPTION = '\
The format used for the `note` field. An empty string signifies unstructured text.'


class ValidateTypeMixin:
    @validates('type')
    def validate_type(self, value):
        if f'{value}Schema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class TransfersList:
    def __init__(self, debtor_id, items):
        self.debtor_id = debtor_id
        self.items = items


class ObjectReferenceSchema(Schema):
    uri = fields.String(
        required=True,
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class ObjectReferencesPageSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/debtors/2/enumerate',
    )
    type = fields.Function(
        lambda obj: 'ObjectReferencesPage',
        required=True,
        type='string',
        description='The type of this object.',
        example='ObjectReferencesPage',
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `ObjectReference`s. Can be empty.',
        example=[{'uri': f'{i}/'} for i in [1, 11, 111]],
    )
    next = fields.String(
        dump_only=True,
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='ObjectReferencesPage'),
        example='?prev=111',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'items' in obj
        return obj


class AuthorityIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='AuthorityIdentity',
        default='AuthorityIdentity',
        description='The type of this object.',
        example='AuthorityIdentity',
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=100),
        format='uri',
        description="The information contained in this field must be enough to uniquely and "
                    "reliably identify the accounting authority that manages this debtor. Note "
                    "that a network request *should not be needed* to identify the accounting "
                    "authority.",
        example='urn:example:authority',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class DebtorIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorIdentity',
        default='DebtorIdentity',
        description='The type of this object.',
        example='DebtorIdentity',
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=100),
        format='uri',
        description="The information contained in this field must be enough to uniquely and "
                    "reliably identify the debtor. Note that a network request *should not "
                    "be needed* to identify the debtor.",
        example='swpt:1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class DebtorsListSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/debtors/.list',
    )
    type = fields.Function(
        lambda obj: 'DebtorsList',
        required=True,
        type='string',
        description='The type of this object.',
        example='DebtorsList',
    )
    items_type = fields.String(
        required=True,
        dump_only=True,
        data_key='itemsType',
        description='The type of the items in the paginated list.',
        example='string',
    )
    first = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description='The URI of the first page in the paginated list. This can be a relative URI. '
                    'The object retrieved from this URI will have: 1) An `items` field (an '
                    'array), which will contain the first items of the paginated list; 2) May '
                    'have a `next` field (a string), which would contain the URI of the next '
                    'page in the list.',
        example='/list?page=1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'itemsType' in obj
        assert 'first' in obj
        return obj


class DebtorReservationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorReservationRequest',
        load_only=True,
        description='The type of this object.',
        example='DebtorReservationRequest',
    )


class DebtorReservationSchema(ValidateTypeMixin, Schema):
    type = fields.Function(
        lambda obj: 'DebtorReservation',
        required=True,
        type='string',
        description='The type of this object.',
        example='DebtorReservation',
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the reservation was created.',
    )
    reservation_id = fields.Function(
        lambda obj: obj.reservation_id or 0,
        required=True,
        data_key='reservationId',
        type='integer',
        format='int64',
        description='A number that will be needed in order to activate the debtor.',
        example=12345,
    )
    debtor_id = fields.Function(
        lambda obj: i64_to_u64(obj.debtor_id),
        required=True,
        data_key='debtorId',
        type='integer',
        format='int64',
        description='The reserved debtor ID.',
        example=1,
    )
    valid_until = fields.Method(
        'get_valid_until_string',
        required=True,
        data_key='validUntil',
        type='string',
        format='date-time',
        description='The moment at which the reservation will expire.',
    )

    def get_valid_until_string(self, obj) -> str:
        calc_reservation_deadline = self.context['calc_reservation_deadline']
        return calc_reservation_deadline(obj.created_at).isoformat()


class DebtorActivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorActivationRequest',
        load_only=True,
        description='The type of this object.',
        example='DebtorActivationRequest',
    )
    optional_reservation_id = fields.Integer(
        load_only=True,
        data_key='reservationId',
        format='int64',
        description='When this field is present, the server will try to activate an existing '
                    'reservation with matching `debtorID` and `reservationID`. When this '
                    'field is not present, the server will try to reserve the debtor ID '
                    'specified in the path, and activate it at once.',
        example=12345,
    )


class DebtorDeactivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorDeactivationRequest',
        load_only=True,
        description='The type of this object.',
        example='DebtorDeactivationRequest',
    )


class InterestRateLowerLimitSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='InterestRateLowerLimit',
        default='InterestRateLowerLimit',
        description='The type of this object.',
        example='InterestRateLowerLimit',
    )
    value = fields.Float(
        required=True,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        description='The annual interest rate (in percents) should be no less than this value.',
    )
    cutoff = fields.Date(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this date.',
    )

    @post_load
    def make_lower_limit(self, data, **kwargs):
        return LowerLimit(value=data['value'], cutoff=data['cutoff'])


class BalanceLowerLimitSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='BalanceLowerLimit',
        default='BalanceLowerLimit',
        description='The type of this object.',
        example='BalanceLowerLimit',
    )
    value = fields.Int(
        required=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        description='The balance should be no less than this value. Normally, '
                    'this will be a negative number.',
    )
    cutoff = fields.Date(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this date.',
    )

    @post_load
    def make_lower_limit(self, data, **kwargs):
        return LowerLimit(value=data['value'], cutoff=data['cutoff'])


class DebtorInfoSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorInfo',
        default='DebtorInfo',
        description='The type of this object.',
        example='DebtorInfo',
    )
    iri = fields.String(
        required=True,
        validate=validate.Length(max=200),
        format='iri',
        description='A link (Internationalized Resource Identifier) referring to a document '
                    'containing information about the debtor.',
        example='https://example.com/1/',
    )
    optional_content_type = fields.String(
        data_key='contentType',
        validate=validate.Length(max=100),
        description='Optional MIME type of the document that the `iri` field refers to.',
        example='text/html',
    )
    optional_sha256 = fields.String(
        validate=validate.Regexp('^[0-9A-F]{64}$'),
        data_key='sha256',
        description='Optional SHA-256 cryptographic hash (Base16 encoded) of the content of '
                    'the document that the `iri` field refers to.',
        example='E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
    )

    @validates('optional_content_type')
    def validate_optional_content_type(self, value):
        assert len(value) <= 100
        if not value.isascii():
            raise ValidationError('Includes non-ASCII characters.')

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'iri' in obj
        return obj


class DebtorCreationOptionsSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorCreationOptions',
        default='DebtorCreationOptions',
        description='The type of this object.',
        example='DebtorCreationOptions',
    )


class DebtorSchema(ValidateTypeMixin, Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/debtors/1/',
    )
    type = fields.String(
        missing='Debtor',
        default='Debtor',
        description='The type of this object.',
        example='Debtor',
    )
    authority = fields.Nested(
        AuthorityIdentitySchema,
        required=True,
        dump_only=True,
        data_key='authority',
        description="The accounting authority that manages this debtor.",
        example={'type': 'AuthorityIdentity', 'uri': 'urn:example:authority'},
    )
    identity = fields.Nested(
        DebtorIdentitySchema,
        required=True,
        dump_only=True,
        data_key='identity',
        description="Debtor's `DebtorIdentity`.",
        example={'type': 'DebtorIdentity', 'uri': 'swpt:2'},
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='transfersList',
        description="The URI of the debtor's list of pending credit-issuing transfers "
                    "(`TransfersList`).",
        example={'uri': '/debtors/2/transfers/'},
    )
    create_transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='createTransfer',
        description='A URI to which the debtor can POST `TransferCreationRequest`s to '
                    'create new credit-issuing transfers.',
        example={'uri': '/debtors/2/transfers/'},
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the debtor was created.',
    )
    balance = fields.Int(
        required=True,
        dump_only=True,
        format="int64",
        description=Debtor.balance.comment,
        example=-1000000,
    )
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        required=True,
        data_key='balanceLowerLimits',
        description=f'Enforced lower limits for the `balance` field.\n\n{ESTABLISHED_LIMITS_NOTE}',
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        required=True,
        data_key='interestRateLowerLimits',
        description=f'Enforced lower limits for the `interestRate` field.\n\n{ESTABLISHED_LIMITS_NOTE}',
    )
    interest_rate_target = fields.Float(
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        required=True,
        data_key='interestRateTarget',
        description='The annual rate (in percents) at which the debtor wants the interest '
                    'to accumulate on creditors\' accounts. The actual current interest rate may '
                    'be different if interest rate limits are being enforced. When the debtor is '
                    'created, the initial value for this field will be `0`.',
        example=0,
    )
    interest_rate = fields.Float(
        required=True,
        dump_only=True,
        data_key='interestRate',
        description="The current annual interest rate (in percents) at which "
                    "interest accumulates on creditors' accounts.",
    )
    optional_deactivated_at = fields.DateTime(
        dump_only=True,
        data_key='deactivatedAt',
        description="The moment at which the debtor was deactivated. If this field is not present, "
                    "this means that the debtor has not been deactivated yet.",
    )
    optional_info = fields.Nested(
        DebtorInfoSchema,
        data_key='info',
        description='Optional link to additional information about the debtor.',
    )

    @pre_dump
    def process_debtor_instance(self, obj, many):
        assert isinstance(obj, Debtor)
        obj = copy(obj)
        obj.uri = url_for(self.context['Debtor'], _external=False, debtorId=obj.debtor_id)
        obj.authority = {'uri': current_app.config['APP_AUTHORITY_URI']}
        obj.identity = {'uri': f'swpt:{i64_to_u64(obj.debtor_id)}'}
        obj.transfers_list = {'uri': url_for(self.context['TransfersList'], _external=False, debtorId=obj.debtor_id)}
        obj.create_transfer = obj.transfers_list

        if obj.deactivated_at is not None:
            obj.optional_deactivated_at = obj.deactivated_at

        if obj.debtor_info_iri is not None:
            debtor_info = {'iri': obj.debtor_info_iri}
            if obj.debtor_info_content_type is not None:
                debtor_info['optional_content_type'] = obj.debtor_info_content_type
            if obj.debtor_info_sha256 is not None:
                debtor_info['optional_sha256'] = b16encode(obj.debtor_info_sha256).decode()
            obj.optional_info = debtor_info

        return obj


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferError',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferError',
    )
    error_code = fields.String(
        required=True,
        dump_only=True,
        data_key='errorCode',
        description='The error code.'
                    '\n\n'
                    '* `"CANCELED_BY_THE_SENDER"` signifies that the transfer has been '
                    '  canceled the sender.\n'
                    '* `"SENDER_DOES_NOT_EXIST"` signifies that the sender\'s account '
                    '  does not exist.\n'
                    '* `"RECIPIENT_IS_UNREACHABLE"` signifies that the recipient\'s'
                    '  account does not exist, or does not accept incoming transfers.\n'
                    '* `"NO_RECIPIENT_CONFIRMATION"` signifies that a confirmation from '
                    '  the recipient is required, but has not been obtained.\n'
                    '* `"TRANSFER_NOTE_IS_TOO_LONG"` signifies that the transfer has been '
                    '  rejected because the byte-length of the transfer note is too big.\n'
                    '* `"INSUFFICIENT_AVAILABLE_AMOUNT"` signifies that the transfer '
                    '  has been rejected due to insufficient amount available on the '
                    '  sender\'s account.\n'
                    '* `"TERMINATED"` signifies that the transfer has been terminated '
                    '  due to expired deadline, unapproved interest rate change, or '
                    '  some other *temporary or correctable condition*. If the client '
                    '  verifies the transer options and retries the transfer, chances '
                    '  are that it will be committed successfully.\n',
        example='INSUFFICIENT_AVAILABLE_AMOUNT',
    )
    total_locked_amount = fields.Method(
        'get_total_locked_amount',
        format="int64",
        data_key='totalLockedAmount',
        description='This field will be present only when the transfer has been rejected '
                    'due to insufficient available amount. In this case, it will contain '
                    'the total sum secured (locked) for transfers on the account, '
                    '*after* this transfer has been finalized.',
        example=0,
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'errorCode' in obj
        return obj

    def get_total_locked_amount(self, obj):
        if obj['error_code'] != SC_INSUFFICIENT_AVAILABLE_AMOUNT:
            return missing
        return obj.get('total_locked_amount') or 0


class TransferResultSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferResult',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferResult',
    )
    finalized_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='finalizedAt',
        description='The moment at which the transfer was finalized.',
    )
    committed_amount = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        data_key='committedAmount',
        description='The transferred amount. If the transfer has been successful, the value will '
                    'be equal to the requested transfer amount (always a positive number). If '
                    'the transfer has been unsuccessful, the value will be zero.',
        example=0,
    )
    error = fields.Nested(
        TransferErrorSchema,
        dump_only=True,
        description='An error that has occurred during the execution of the transfer. This field '
                    'will be present if, and only if, the transfer has been unsuccessful.',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'finalizedAt' in obj
        assert 'committedAmount' in obj
        return obj


class TransferCreationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='TransferCreationRequest',
        default='TransferCreationRequest',
        description='The type of this object.',
        example='TransferCreationRequest',
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    recipient_creditor_id = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='int64',
        data_key='creditorId',
        description="The recipient's creditor ID.",
        example=1111,
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The amount to be transferred. Must be positive.',
        example=1000,
    )
    transfer_note_format = fields.String(
        missing='',
        validate=validate.Regexp(TRANSFER_NOTE_FORMAT_REGEX),
        data_key='noteFormat',
        description=TRANSFER_NOTE_FORMAT_DESCRIPTION,
        example='',
    )
    transfer_note = fields.String(
        missing='',
        validate=validate.Length(max=TRANSFER_NOTE_MAX_BYTES),
        data_key='note',
        description='A note from the debtor. Can be any string that the debtor wants the '
                    'recipient to see.',
        example='Hello, World!',
    )

    @validates('transfer_note')
    def validate_transfer_note(self, value):
        if len(value.encode('utf8')) > TRANSFER_NOTE_MAX_BYTES:
            raise ValidationError(f'The total byte-length of the note exceeds {TRANSFER_NOTE_MAX_BYTES} bytes.')


class TransferSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/debtors/1/transfers/123e4567-e89b-12d3-a456-426655440000',
    )
    type = fields.Function(
        lambda obj: 'Transfer',
        required=True,
        type='string',
        description='The type of this object.',
        example='Transfer',
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='transfersList',
        description="The URI of creditor's `TransfersList`.",
        example={'uri': '/debtors/2/transfers/'},
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    recipient_creditor_id = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='int64',
        data_key='creditorId',
        description="The recipient's creditor ID.",
        example=1111,
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The amount to be transferred. Must be positive.',
        example=1000,
    )
    transfer_note_format = fields.String(
        required=True,
        dump_only=True,
        data_key='noteFormat',
        pattern=TRANSFER_NOTE_FORMAT_REGEX,
        description=TRANSFER_NOTE_FORMAT_DESCRIPTION,
        example='',
    )
    transfer_note = fields.String(
        required=True,
        dump_only=True,
        data_key='note',
        description='A note from the debtor. Can be any string that the debtor wants the '
                    'recipient to see.',
        example='Hello, World!',
    )
    initiated_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description='The moment at which the transfer was initiated.',
    )
    checkup_at = fields.Method(
        'get_checkup_at_string',
        type='string',
        format='date-time',
        data_key='checkupAt',
        description="The moment at which the debtor is advised to look at the transfer "
                    "again, to see if it's status has changed. If this field is not present, "
                    "this means either that the status of the transfer is not expected to "
                    "change, or that the moment of the expected change can not be predicted.",
    )
    result = fields.Nested(
        TransferResultSchema,
        dump_only=True,
        description='Contains information about the outcome of the transfer. This field will '
                    'be preset if, and only if, the transfer has been finalized. Note that a '
                    'finalized transfer can be either successful, or unsuccessful.',
    )

    @pre_dump
    def process_initiated_transfer_instance(self, obj, many):
        assert isinstance(obj, RunningTransfer)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['Transfer'],
            _external=False,
            debtorId=obj.debtor_id,
            transferUuid=obj.transfer_uuid,
        )
        obj.transfers_list = {'uri': url_for(self.context['TransfersList'], _external=False, debtorId=obj.debtor_id)}

        if obj.finalized_at:
            result = {'finalized_at': obj.finalized_at}

            error_code = obj.error_code
            if error_code is None:
                result['committed_amount'] = obj.amount
            else:
                result['committed_amount'] = 0
                result['error'] = {'error_code': error_code, 'total_locked_amount': obj.total_locked_amount}

            obj.result = result

        return obj

    def get_checkup_at_string(self, obj):
        if obj.finalized_at:
            return missing

        calc_checkup_datetime = self.context['calc_checkup_datetime']
        return calc_checkup_datetime(obj.debtor_id, obj.initiated_at).isoformat()


class TransferCancelationRequestSchema(Schema):
    type = fields.String(
        missing='TransferCancelationRequest',
        default='TransferCancelationRequest',
        description='The type of this object.',
        example='TransferCancelationRequest',
    )


class TransfersListSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/debtors/1/transfers/',
    )
    type = fields.Function(
        lambda obj: 'TransfersList',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransfersList',
    )
    debtor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Debtor`.",
        example={'uri': '/debtors/1/'},
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        description='Contains links to all `Transfers` in an array of `ObjectReference`s.',
        example=[{'uri': i} for i in [
            '123e4567-e89b-12d3-a456-426655440000',
            '183ea7c7-7a96-4ed7-a50a-a2b069687d23',
        ]],
    )
    itemsType = fields.Function(
        lambda obj: 'ObjectReference',
        required=True,
        type='string',
        description='The type of the items in the list.',
        example='ObjectReference',
    )
    first = fields.Function(
        lambda obj: '',
        required=True,
        type='string',
        format="uri-reference",
        description='This will always be an empty string, representing the relative URI of '
                    'the first and only page in a paginated list.',
        example='',
    )

    @pre_dump
    def process_transfers_collection_instance(self, obj, many):
        assert isinstance(obj, TransfersList)
        obj = copy(obj)
        obj.uri = url_for(self.context['TransfersList'], _external=False, debtorId=obj.debtor_id)
        obj.debtor = {'uri': url_for(self.context['Debtor'], _external=False, debtorId=obj.debtor_id)}
        obj.items = [{'uri': uri} for uri in obj.items]

        return obj
