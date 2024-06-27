"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from typing import Any, Dict
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError


class JWT:
    def __init__(self):
        self.algorithm = 'HS512'
        self.signature = 'learn_programming_yourself'
        self.jwt_options = {
            'verify_signature': True,
            'verify_exp': True,
            'verify_nbf': False,
            'verify_iat': True,
            'verify_aud': False
        }

    def encode(self, data: Dict[str, Any]):
        jwt_token = jwt.encode(data, self.signature, algorithm=self.algorithm)
        return jwt_token

    def decode(self, encoded_jwt: str):
        try:
            decoded_data = jwt.decode(encoded_jwt, self.signature, algorithms=self.algorithm, options=self.jwt_options)
            return decoded_data
        except InvalidSignatureError as e:
            return {'errors': e}
        except ExpiredSignatureError as e:
            return {'errors': e}
