"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations


class Events:
    def __init__(self, type: str):
        self.type = type

    def create(self):
        pass

    def fetch(self):
        pass

    def delete(self):
        pass


class Errors:
    def __init__(self, type: str):
        self.type = type

    def create(self):
        pass

    def fetch(self):
        pass

    def delete(self):
        pass


class Notifications:
    def __init__(self, type: str):
        self.type = type

    def create(self):
        pass

    def fetch(self):
        pass

    def delete(self):
        pass


class Actions:
    def __init__(self, type: str):
        self.type = type

    def create(self):
        pass

    def fetch(self):
        pass

    def delete(self):
        pass
