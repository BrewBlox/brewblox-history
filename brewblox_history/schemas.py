"""
Schemas used in API endpoints
"""

from marshmallow import Schema, ValidationError, fields, validates_schema


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


class HistoryValuesSchema(HistoryQuerySchema):
    start = fields.String(required=False)
    duration = fields.String(required=False)
    end = fields.String(required=False)

    limit = fields.Integer(required=False)
    order_by = fields.String(required=False)


class HistorySSEValuesSchema(HistoryQuerySchema):
    start = fields.String(required=False)
    duration = fields.String(required=False)
    end = fields.String(required=False)


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
        print(data)
        if not isinstance(data['data'], (dict, list)):
            raise ValidationError('"data" field must be a dict or list')
        return data
