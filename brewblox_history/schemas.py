"""
Schemas used in API endpoints
"""

from marshmallow import (Schema, ValidationError, fields, validate,
                         validates_schema)


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


class MQTTStateSchema(Schema):
    key = fields.String(required=True)
    type = fields.String(required=True)
    ttl = fields.String(required=False)
    data = fields.Raw(required=True)

    @validates_schema
    def check_data_field(self, data, **kwargs):
        if not isinstance(data['data'], (dict, list)):
            raise ValidationError('"data" field must be a dict or list')
        return data
