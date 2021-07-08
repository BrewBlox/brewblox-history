"""
Schemas used in API endpoints
"""

from marshmallow import Schema, ValidationError, fields
from marshmallow.utils import INCLUDE
from marshmallow.validate import OneOf, Regexp


def validate(schema: Schema, data: dict):
    errors = schema.validate(data)
    if errors:
        raise ValidationError(errors)
    return data


class FieldsQuerySchema(Schema):
    measurement = fields.String(required=False)
    include_stale = fields.Boolean(required=False, default=False)


class HistoryQuerySchema(Schema):
    measurement = fields.String(required=True)
    fields_ = fields.List(fields.String(),
                          data_key='fields',
                          attribute='fields',
                          required=False)
    approx_points = fields.Integer(required=False)
    policy = fields.String(required=False)
    epoch = fields.String(required=False,
                          validate=OneOf(['ns', 'u', 'µ', 'ms', 's', 'm', 'h']))

    start = fields.Raw(required=False)
    duration = fields.Raw(required=False)
    end = fields.Raw(required=False)

    limit = fields.Integer(required=False)
    order_by = fields.String(required=False)


class HistoryDebugQuerySchema(Schema):
    query = fields.String(required=True)


class HistoryStreamCommandSchema(Schema):
    id = fields.String(required=True)
    command = fields.String(required=True,
                            validate=OneOf([
                                'values',
                                'last_values',
                                'stop'
                            ]))
    query = fields.Dict(keys=fields.Str(),
                        required=False)


class MQTTHistorySchema(Schema):
    key = fields.String(required=True)
    data = fields.Dict(required=True)


class DatastoreValueSchema(Schema):
    class Meta:
        unknown = INCLUDE
    namespace = fields.String(required=True,
                              validate=Regexp(r'^[\w\-\.\:~_]*$'))
    id = fields.String(required=True,
                       validate=Regexp(r'^[\w\-\.~_]+$'))


class DatastoreSingleQuerySchema(Schema):
    namespace = fields.String(required=True)
    id = fields.String(required=True)


class DatastoreMultiQuerySchema(Schema):
    namespace = fields.String(required=True)
    ids = fields.List(fields.String(), required=False)
    filter = fields.String(required=False)


class DatastoreSingleValueSchema(Schema):
    value = fields.Nested(DatastoreValueSchema(), required=True)


class DatastoreMultiValueSchema(Schema):
    values = fields.Nested(DatastoreValueSchema(many=True), required=True)


class DatastoreDeleteResponseSchema(Schema):
    count = fields.Integer(required=True)


class TimeSeriesFieldsQuerySchema(Schema):
    start = fields.String(required=False)


class TimeSeriesRangesQuerySchema(Schema):
    fields_ = fields.List(fields.String(),
                          data_key='fields',
                          attribute='fields',
                          required=True)
    start = fields.String(required=False)
    end = fields.String(required=False)
    duration = fields.String(required=False)
    step = fields.String(required=False)


class TimeSeriesMetricsQuerySchema(Schema):
    fields_ = fields.List(fields.String(),
                          data_key='fields',
                          attribute='fields',
                          required=True)


class TimeSeriesStreamCommandSchema(Schema):
    id = fields.String(required=True)
    command = fields.String(required=True,
                            validate=OneOf([
                                'ranges',
                                'metrics',
                                'stop'
                            ]))
    query = fields.Dict(keys=fields.Str(),
                        required=False)
