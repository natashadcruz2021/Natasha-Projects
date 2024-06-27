"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""


class InvalidInputError(Exception):
    """Raised when the user input is clashing with the system-defined keywords"""
    def __int__(self, message: str, error_code: str):
        self.message = message
