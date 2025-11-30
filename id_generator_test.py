from id_generator import RandomGenerator, NodeAwareGenerator

import unittest as ut


class TestRandomGenerator(ut.TestCase):
    def test_unique_ids(self):

        generator = RandomGenerator()
        n = 1000
        ids = set(generator.generate() for _ in range(n))
 
        self.assertEqual(len(ids), n)

