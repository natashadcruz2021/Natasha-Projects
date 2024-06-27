"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from typing import Any, List, Dict


class Node:
    def __init__(self, data: Dict[str, Any] = None, next: Node = None):
        self.data = data
        self.next = next


class LinkedList:
    def __init__(self):
        self.head = None

    def insert_at_start(self, data: Dict[str, Any]):
        node = Node(data=data, next=self.head)
        self.head = node

    def insert_at_end(self, data: Dict[str, Any]):
        if self.head is None:
            self.head = Node(data=data, next=None)
            return

        iterator = self.head
        while iterator.next:
            iterator = iterator.next

        iterator.next = Node(data=data, next=None)

    def insert(self, data: Dict[str, Any], index: int):
        if index < 0 or index > self.length():
            raise IndexError('Incorrect index provided.')

        if index == 0:
            self.insert_at_start(data)
            return
        elif index == self.length():
            self.insert_at_end(data)
            return

        count = 0
        iterator = self.head

        while iterator:
            iterator = iterator.next
            if count == index-1:
                node = Node(data=data, next=iterator.next)
                iterator.next = node
                break
            count += 1

        return

    def remove(self, index: int):
        if index < 0 or index > self.length():
            raise IndexError('Incorrect index provided.')

        if index == 0:
            self.head = self.head.next
            return

        count = 0
        iterator = self.head

        while iterator:
            if count == index-1:
                iterator.next = iterator.next.next
                break
            iterator = iterator.next
            count += 1

        return

    def print(self) -> List[Dict[str, Any]]:
        linked_list = []
        if self.head is not None:
            iterator = self.head
            while iterator:
                linked_list.append(iterator.data)
                iterator = iterator.next
        return linked_list

    def length(self) -> int:
        count = 0
        iterator = self.head
        while iterator:
            count += 1
            iterator = iterator.next
        return count


llist = LinkedList()
llist.insert_at_start({'1': '1'})
llist.insert_at_end({'2': '2'})
llist.insert_at_end({'3': '3'})
llist.insert_at_start({'4': '4'})

print(llist.print())
print(llist.length())
llist.remove(2)
print(llist.print())
llist.insert({'new_node': 'new_node'}, 3)
print(llist.print())
