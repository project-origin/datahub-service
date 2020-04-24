"""
Additional common schema validators for JSON-to-model validation.
Plugs into Marshmallow's validation model and can be used in
conjunction with the marshmallow and marshmallow-dataclass libraries.
"""
from marshmallow import ValidationError


def unique_values(list_of_values, *args, **kwargs):
    """
    Validates that a list of items are unique,
    ie. no value are present more than once.
    """
    errors = []

    for value in set(list_of_values):

        if list_of_values.count(value) > 1:
            errors.append('Value must be unique: %s occurred %d times.' % (
                value, list_of_values.count(value)))

    if errors:
        raise ValidationError(errors)
