import unittest as ut
import random
from blockchain import Block, Blockchain, BlockReference


class TestBlock(ut.TestCase):
    def test_serialize_deserialize(self):
        """
        A Block can be serialized and deserialized without changing it.
        """
        block = Block.create_new(
            BlockReference("", 0),
            0,
            0,
            100,
            []
        )
        self.assertEqual(block, Block.from_dict(block.to_dict()))

    def test_random_hash(self):
        """
        with some seeds, the block hash ends in zeros.
        """
        rand = random.Random(42)
        successes = []
        for i in range(100):
            r = rand.randint(0,1000)
            block = Block.create_new(
                BlockReference("", 0),
                0,
                0,
                r,
                []
            )
            if int(block.hashvalue, 16) % 16 == 0:
                successes.append(r)
        self.assertGreater(len(successes), 0)



class TestBlockchain(ut.TestCase):
    def test_new_blockchain_genesis_block(self):
        """
        by definition the index of the genesis block is 0
           and it has no events.
        by convention the origin of the genesis block is 0.
        """

        chain = Blockchain.create_new()

        self.assertEqual(chain.get_head_block().events, [])
        self.assertEqual(chain.get_head_block().index, 0)
        self.assertEqual(chain.get_head_block().origin, 0)

    def test_empty_longest_chain(self):
        """
        the longest chain of a new blockchain is empty.
        (since the genesis block is not part of the chain)
        """
        chain = Blockchain.create_new()
        longest_chain = list(chain.get_longest_chain())
        self.assertEqual(longest_chain, [])

    def test_add_event_1(self):
        """
        adding an event to the top of the blockchain extends the longest and
        sets the head_block.
        """
        chain = Blockchain.create_new()
        block1 = Block.create_new(
            chain.get_head_block().get_reference(),
            1,
            1,
            120,
            []
            )
        chain.add_block(block1)
        self.assertEqual(chain.get_head_block(), block1)
        longest_chain = list(chain.get_longest_chain())
        self.assertEqual(longest_chain, [block1])
        block2 = Block.create_new(
            block1.get_reference(),
            3,
            2,
            121,
            []
        )
        chain.add_block(block2)
        self.assertEqual(chain.get_head_block(), block2)
        longest_chain = list(chain.get_longest_chain())
        self.assertEqual(longest_chain, [block1,block2])

    def test_add_event_2(self):
        """
        adding two blocks on top of the same blocks creates competing longest chains.
        one is choosen.
        """
        chain = Blockchain.create_new()
        block1 = Block.create_new(
            chain.get_head_block().get_reference(),
            1,
            1,
            120,
            []
            )
        block2 = Block.create_new(
            chain.get_head_block().get_reference(),
            2,
            1,
            124,
            []
            )
        chain.add_block(block1)
        chain.add_block(block2)
        self.assertIn(chain.get_head_block(), [block1, block2])
        longest_chain = list(chain.get_longest_chain())
        self.assertIn(longest_chain, [[block1], [block2]])

    def test_add_event_3(self):
        """
        adding an event to the top of the blockchain extends the longest and
        sets the head_block.
        """
        chain = Blockchain.create_new()
        block1 = Block.create_new(
            chain.get_head_block().get_reference(),
            1,
            1,
            120,
            []
            )
        block2 = Block.create_new(
            block1.get_reference(),
            3,
            2,
            121,
            []
        )
        chain.add_block(block2)
        self.assertEqual(chain.get_head_block().index, 0)
        longest_chain = list(chain.get_longest_chain())
        self.assertEqual(longest_chain, [])
        chain.add_block(block1)
        self.assertEqual(chain.get_head_block(), block2)
        longest_chain = list(chain.get_longest_chain())
        self.assertEqual(longest_chain, [block1, block2])
