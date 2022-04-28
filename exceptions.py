class HashNoSalt(Exception):
    """Exception to raise when trying to hash without salt set up in environment"""


class CastException(Exception):
    """Exception to raise when calue cannot be casted"""
