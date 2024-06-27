"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import base64
from Crypto import Random
from Crypto.Util.Padding import pad, unpad
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes


class Cryptography:
    def __init__(self):
        self.private_key = b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        self.block_size = AES.block_size
        self.init_vector = Random.new().read(self.block_size)

    def encrypt(self, data: str):
        cipher = AES.new(self.private_key, AES.MODE_CBC, self.init_vector)
        byte_data = bytes(data, 'utf-8')
        print('<--- BYTE DATA --->', byte_data)
        encrypted_data = cipher.encrypt(pad(byte_data, 16, style='pkcs7'))
        print('<--- BYTE ARRAY TO BYTES DATA --->', bytes(encrypted_data), list(encrypted_data))
        b64encoded_data = self.b64enc(encrypted_data)
        return b64encoded_data

    def decrypt(self, encrypted_data):
        b64decoded_data = self.b64dec(encrypted_data)
        cipher = AES.new(self.private_key, AES.MODE_CBC, self.init_vector)
        data = unpad(cipher.decrypt(b64decoded_data), 16)
        return data.decode('utf-8')

    @staticmethod
    def b64enc(value: str):
        value_bytes = value.encode('utf-8')
        base64_bytes = base64.b64encode(value_bytes)
        base64_string = base64_bytes.decode('utf-8')
        return base64_string

    @staticmethod
    def b64dec(base64_string: str):
        base64_bytes = base64_string.encode('utf-8')
        value_bytes = base64.b64decode(base64_bytes)
        value = value_bytes.decode('utf-8')
        return value
