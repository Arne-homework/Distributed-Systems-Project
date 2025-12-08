from vector_clock import VectorTimestamp, VectorClock
import unittest as ut


class TestVectorTimestamp(ut.TestCase):
    def test_repr(self):
        """VectorTimestamp has a meaningful representation"""
        sut = VectorTimestamp([1, 2, 3, 4])
        self.assertEqual(repr(sut), "VectorTimestamp([1, 2, 3, 4])")

    def test_eq(self):
        """VectorTimestamps of identical vectors are equal"""
        sut0 = VectorTimestamp([1, 2, 3, 4])
        sut1 = VectorTimestamp([1, 2, 3, 4])
        self.assertTrue(sut0 == sut1)
        self.assertFalse(sut0 != sut1)

    def test_not_eq(self):
        """VectorTimestamps over different Vectors are not equal"""
        sut0 = VectorTimestamp([1, 2, 3, 5])
        sut1 = VectorTimestamp([1, 2, 3, 4])
        self.assertFalse(sut0 == sut1)
        self.assertTrue(sut0 != sut1)

    def test_format(self):
        """
        VectorTimestamp can be converted
        to and from integer vectors
        """
        vector = [1, 2, 3, 4]
        sut0 = VectorTimestamp(vector[:])
        sut1 = VectorTimestamp.from_list(vector[:])
        self.assertEqual(sut0, sut1)

        self.assertEqual(vector, sut0.to_list())
        self.assertEqual(vector, sut0.vector)

    def test_lt(self):
        """
        VectorTimestamps are less than comparable.
        """
        sut0 = VectorTimestamp([1, 2, 3, 4])
        sut1 = VectorTimestamp([1, 2, 3, 5])

        self.assertTrue(sut0 < sut1)
        self.assertFalse(sut1 < sut0)

    def test_eq_no_lt(self):
        """
        Equal VectorTimestamps are not less than.
        """
        sut0 = VectorTimestamp([1, 2, 3, 4])
        sut1 = VectorTimestamp([1, 2, 3, 4])

        self.assertFalse(sut0 < sut1)
        self.assertFalse(sut1 < sut0)

    def test_sort(self):
        """
        VectorTimestamps can be sorted.
        """
        timestamps = [VectorTimestamp([3, 3, 2, 2]),
                      VectorTimestamp([1, 2, 3, 4]),
                      VectorTimestamp([0, 0, 0, 0])]
        sorted_timestamps = sorted(timestamps)
        # since python sorting is stable by default.
        # The order of concurrent timestamps stays unchanged.
        self.assertEqual(sorted_timestamps,
                         [VectorTimestamp([0, 0, 0, 0]),
                          VectorTimestamp([3, 3, 2, 2]),
                          VectorTimestamp([1, 2, 3, 4])])

    def test_concurrent(self):
        """
        There are VectorTimestamps that are not equal but also not less than ordered.
        """
        sut0 = VectorTimestamp([1, 1, 3, 5])
        sut1 = VectorTimestamp([1, 2, 3, 4])

        self.assertFalse(sut0 < sut1)
        self.assertFalse(sut1 < sut0)
        self.assertFalse(sut1 == sut0)
        self.assertTrue(sut0.is_concurrent(sut1))

    def test_eq_concurrent(self):
        """
        Equal VectorTimestamps are treated as concurrent.
        """
        sut0 = VectorTimestamp([1, 2, 3, 4])
        sut1 = VectorTimestamp([1, 2, 3, 4])

        self.assertTrue(sut1 == sut0)
        self.assertTrue(sut0.is_concurrent(sut1))


class TestVectorClock(ut.TestCase):
    def _create_timestamp_from_vector(self, vector):
        return VectorTimestamp(vector)

    def test_generate_timestamp(self):
        """
        A VectorClock can be initialized from the node id and the number of nodes.


        """
        sut0 = VectorClock.create_new(0, 10)

        self.assertTrue(
            isinstance(
                sut0.current_timestamp, VectorTimestamp))
        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector([0, 0, 0, 0, 0,
                                                0, 0, 0, 0, 0,]
                                               )
        )

    def test_increment(self):
        """
        VectorClock.increment increases the counter for the current node.
        """
        sut0 = VectorClock.create_new(0, 4)

        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector(
                [0, 0, 0, 0, ]
            )
        )
        sut0.increment()
        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector(
                [1, 0, 0, 0,]
                )
            )

    def test_update(self):
        """
        VectorClock.update updates all counters
        to the maximum with the other timestamp
        AND increments the own counter.
        """
        sut0 = VectorClock.create_new(0, 4)
        timestamp1 = self._create_timestamp_from_vector([0, 2, 3, 5])
        timestamp2 = self._create_timestamp_from_vector([3, 1, 3, 4])
        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector(
                [0, 0, 0, 0, ]
            )
        )
        sut0.update(timestamp1)
        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector(
                [1, 2, 3, 5,]
            )
        )
        sut0.update(timestamp2)
        # Note: despite our own counter being larger we don't throw.
        self.assertEqual(
            sut0.current_timestamp,
            self._create_timestamp_from_vector(
                [4, 2, 3, 5,]
            )
        )
