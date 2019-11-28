from collections import abc
from marshmallow import Schema, fields, validate, pre_dump, missing
from flask import url_for
from .models import Debtor, PendingTransfer, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64
from swpt_lib import endpoints


class ResourceSchema(Schema):
    id = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description="The URI of this object.",
        example='https://example.com/resources/123',
    )
    type = fields.Method(
        'get_type',
        required=True,
        type='string',
        description='The type of this object.',
        example='Resource',
    )

    def get_type(self, obj):  # pragma: no cover
        raise NotImplementedError

    def get_uri(self, obj):  # pragma: no cover
        raise NotImplementedError


class CollectionSchema(Schema):
    members = fields.List(
        fields.Str(format='uri-reference'),
        required=True,
        dump_only=True,
        description='A list of URIs for the contained items.',
        example=['111111', '222222', '333333'],
    )
    totalItems = fields.Function(
        lambda obj: len(obj['members']),
        required=True,
        type='number',
        format='int32',
        description='The total number of items in the collection.',
        example=3,
    )

    @pre_dump
    def _to_dict(self, obj, many):
        assert not many
        assert isinstance(obj, abc.Iterable)
        return {'members': obj}


class InterestRateLowerLimitSchema(Schema):
    value = fields.Float(
        required=True,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        description='The annual interest rate (in percents) should be no less than this value.',
    )
    cutoff = fields.DateTime(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this moment.',
    )


class BalanceLowerLimitSchema(Schema):
    value = fields.Int(
        required=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        description='The balance should be no less than this value.',
    )
    cutoff = fields.DateTime(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this moment.',
    )


class DebtorCreationRequestSchema(Schema):
    debtor_id = fields.Int(
        required=True,
        data_key='debtorId',
        format="int64",
        description="The debtor's ID",
        example=1,
    )


class DebtorSchema(ResourceSchema):
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description=Debtor.created_at_date.comment,
    )
    balance = fields.Int(
        required=True,
        dump_only=True,
        format="int64",
        description=Debtor.balance.comment,
    )
    balance_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='balanceTimestamp',
        description='The moment at which the last change in the `balance` field happened.',
    )
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        required=True,
        dump_only=True,
        data_key='balanceLowerLimits',
        description='Enforced lower limits for the `balance` field.',
    )
    interest_rate_target = fields.Float(
        required=True,
        dump_only=True,
        data_key='interestRateTarget',
        description=Debtor.interest_rate_target.comment,
        example=0,
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        required=True,
        dump_only=True,
        data_key='interestRateLowerLimits',
        description='Enforced interest rate lower limits.',
    )
    interest_rate = fields.Float(
        required=True,
        dump_only=True,
        data_key='interestRate',
        description="The current annual interest rate (in percents) at which "
                    "interest accumulates on creditors' accounts.",
    )
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the debtor is currently active or not."
    )

    def get_type(self, obj):
        return 'Debtor'

    def get_uri(self, obj):
        return url_for(self.context['endpoint'], debtorId=obj.debtor_id)


class DebtorPolicySchema(DebtorSchema):
    def get_type(self, obj):
        return 'DebtorPolicy'

    def get_uri(self, obj):
        return url_for(self.context['endpoint'], debtorId=obj.debtor_id)


class DebtorPolicyUpdateRequestSchema(Schema):
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        missing=[],
        data_key='balanceLowerLimits',
        description='Additional balance lower limits to enforce.',
    )
    interest_rate_target = fields.Float(
        missing=None,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        data_key='interestRateTarget',
        description=Debtor.interest_rate_target.comment,
        example=0,
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        missing=[],
        data_key='interestRateLowerLimits',
        description='Additional interest rate lower limits to enforce.',
    )


class TransferErrorSchema(Schema):
    error_code = fields.String(
        required=True,
        dump_only=True,
        data_key='code',
        description='The error code.',
        example='ACC003',
    )
    message = fields.String(
        required=True,
        dump_only=True,
        description='The error message.',
        example='The recipient account does not exist.',
    )


class TransferInfoSchema(Schema):
    recipient = fields.Method(
        'get_recipient_uri',
        required=True,
        type='string',
        format="uri-reference",
        description="The recipient's URI.",
        example='/creditors/1111',
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=PendingTransfer.amount.comment,
        example=1000,
    )
    transfer_info = fields.Dict(
        required=True,
        data_key='transferInfo',
        description=PendingTransfer.transfer_info.comment,
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        data_key='initiatedAt',
        description=PendingTransfer.initiated_at_ts.comment,
    )
    is_finalized = fields.Boolean(
        required=True,
        data_key='isFinalized',
        description='Whether the transfer has been finalized or not.',
        example=True,
    )
    finalizedAt = fields.Function(
        lambda obj: obj.finalized_at_ts or missing,
        type='string',
        format='date-time',
        description='The moment at which the transfer has been finalized. If the transfer '
                    'has not been finalized yet, this field will not be present.',
    )
    is_successful = fields.Boolean(
        required=True,
        data_key='isSuccessful',
        description=PendingTransfer.is_successful.comment,
        example=False,
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        required=True,
        description='Errors that occurred during the transfer.'
    )

    def get_recipient_uri(self, obj):
        return endpoints.build_url('creditor', creditorId=obj.recipient_creditor_id)


class TransfersCollectionSchema(ResourceSchema, CollectionSchema):
    def get_type(self, obj):
        return 'TransfersCollection'

    def get_uri(self, obj):
        return url_for(self.context['endpoint'], debtorId=obj.debtor_id)


class TransferCreationRequestSchema(TransferInfoSchema):
    class Meta:
        fields = [
            'transfer_uuid',
            'recipient',
            'amount',
            'transfer_info',
        ]

    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    recipient = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        format='uri-reference',
        description="The URI of the recipient of the transfer. Can be relative.",
        example='/creditors/1111',
    )
    transfer_info = fields.Dict(
        missing={},
        data_key='transferInfo',
        description=PendingTransfer.transfer_info.comment,
    )


class TransferSchema(ResourceSchema, TransferInfoSchema):
    class Meta:
        dump_only = [
            'recipient',
            'amount',
            'transfer_info',
            'initiated_at_ts',
            'isFinalized',
            'finalizedAt',
            'is_successful',
            'errors',
        ]

    def get_type(self, obj):
        return 'Transfer'

    def get_uri(self, obj):
        return url_for(self.context['endpoint'], debtorId=obj.debtor_id, transferUuid=obj.transfer_uuid)