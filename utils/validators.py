# utils/validators.py
def validate_positive_int(value: int) -> bool:
    return isinstance(value, int) and value > 0
