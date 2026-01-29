# coding=utf-8
import hashlib
from dataclasses import dataclass
from event import Event
from typing import Iterable, Self


@dataclass
class BlockReference:
    """Reference to another block"""
    hashvalue: str  # hash of the block
    origin: int  # node generating this block.

    def to_str(self) -> str:
        """generating a string to be digested in the hashing"""
        return f"({self.hashvalue},{self.origin})"

    def to_dict(self) -> dict:
        return {
            "hashvalue": self.hashvalue,
            "origin": self.origin,
            }

    @staticmethod
    def from_dict(dict_) -> Self:
        return BlockReference(
            dict_["hashvalue"],
            dict_["origin"]
            )


@dataclass
class Block:
    parent_block: BlockReference
    origin: int
    index: int
    seed: int
    hashvalue: str
    events: list[Event]

    def __hash__(self) -> int:
        return int(self.hashvalue, 16)

    def to_str(self) -> str:
        event_str = ",".join(e.to_str() for e in self.events)
        return (f"{self.parent_block.to_str()};"
                f"{self.origin};{self.index};"
                f"{self.seed};"
                f"[{event_str}]")

    @staticmethod
    def create_new(
            parent_block: BlockReference,
            origin: int,
            index: int,
            seed: int,
            events: list[Event]
            ) -> Self:
        h = hashlib.sha512(
            Block(parent_block, origin, index, seed, "", events)
            .to_str()
            .encode()
        )
        hashvalue = h.hexdigest()
        return Block(parent_block, origin, index, seed, hashvalue, events)

    def to_dict(self) -> dict:
        return {
            "parent_block": self.parent_block.to_dict(),
            "origin": self.origin,
            "index": self.index,
            "seed": self.seed,
            "hashvalue": self.hashvalue,
            "events": [event.to_dict() for event in self.events]
        }

    @staticmethod
    def from_dict(dict_) -> Self:
        return Block(
            BlockReference.from_dict(dict_["parent_block"]),
            int(dict_["origin"]),
            int(dict_["index"]),
            int(dict_["seed"]),
            str(dict_["hashvalue"]),
            [Event.from_dict(d) for d in dict_["events"]]
        )

    def get_reference(self):
        return BlockReference(self.hashvalue, self.origin)


class Blockchain:
    CHILD_BLOCK = "Child"
    ORPHAN_BLOCK = "Orphan"

    def __init__(self, genesis_block):
        assert genesis_block.index == 0
        self._chain_blocks: dict[str, Block] = \
            {genesis_block.hashvalue: genesis_block}
        self._genesis_hashvalue: str = genesis_block.hashvalue

        self._head_hashvalue: str = genesis_block.hashvalue
        self._orphan_blocks: set[Block] = set()

    @staticmethod
    def create_new() -> Self:
        return Blockchain(
            Block.create_new(
                BlockReference("", 0), 0, 0, 0, [])
        )

    def add_block(self, block: Block) -> str:
        """
        Add a block to the Blockchain.
        returns CHILD_BLOCK if the parent block is already known.
        return ORPHAN_BLOCK if the parent block is unknown.
        """
        if block.parent_block.hashvalue in self._chain_blocks:
            assert block.parent_block.origin == \
                self._chain_blocks[
                    block.parent_block.hashvalue].origin
            assert self._chain_blocks[
                    block.parent_block.hashvalue].index + 1 == block.index
            self._chain_blocks[block.hashvalue] = block
            if block.index > self._chain_blocks[self._head_hashvalue].index:
                self._head_hashvalue = block.hashvalue
            block_reference = BlockReference(block.hashvalue, block.origin)
            for orphan_block in self._orphan_blocks.copy():
                if orphan_block.parent_block == block_reference:
                    ret = self.add_block(orphan_block)
                    assert ret == self.CHILD_BLOCK
            return self.CHILD_BLOCK
        else:
            self._orphan_blocks.add(block)
            return self.ORPHAN_BLOCK

    def get_longest_chain(self) -> Iterable[Block]:
        """
        Return an iterable over the longest chain.
        """
        longest_chain = []
        next_hashvalue = self._head_hashvalue
        while next_hashvalue != self._genesis_hashvalue:
            block = self._chain_blocks[next_hashvalue]
            longest_chain.append(block)
            next_hashvalue = block.parent_block.hashvalue
        return reversed(longest_chain)

    def get_head_block(self) -> Block:
        """
        return the head block of the longest chain.
        """
        return self._chain_blocks[self._head_hashvalue]
