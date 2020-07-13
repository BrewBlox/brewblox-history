"""
Schemas used in API endpoints
"""

from marshmallow import Schema, fields, validate


class ObjectsQuerySchema(Schema):
    database = fields.String(required=False)
    measurement = fields.String(required=False)


class HistoryMeasurementSchema(Schema):
    database = fields.String(required=False)
    measurement = fields.String(required=True)


class HistoryQuerySchema(HistoryMeasurementSchema):
    fields_ = fields.List(fields.String(),
                          data_key='fields',
                          attribute='fields',
                          required=False)
    approx_points = fields.Integer(required=False)
    policy = fields.String(required=False)
    epoch = fields.String(required=False,
                          validate=validate.OneOf(['ns', 'u', 'µ', 'ms', 's', 'm', 'h']))


class HistoryBoundedValuesSchema(HistoryQuerySchema):
    start = fields.Raw(required=False)
    duration = fields.Raw(required=False)
    end = fields.Raw(required=False)


class HistoryValuesSchema(HistoryBoundedValuesSchema):
    limit = fields.Integer(required=False)
    order_by = fields.String(required=False)


class HistorySSEValuesSchema(HistoryBoundedValuesSchema):
    pass


class HistoryLastValuesSchema(HistoryQuerySchema):
    duration = fields.String(required=False)


class DebugQuerySchema(Schema):
    database = fields.String(required=False)
    query = fields.String(required=True)


class MQTTHistorySchema(Schema):
    key = fields.String(required=True)
    data = fields.Dict(required=True)
