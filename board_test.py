import unittest as ut
from board import Board


class TestBoard(ut.TestCase):
    def test_create_board(self):
        """
        when creating a board it is empty.
        """
        board = Board()

        self.assertEqual(board.get_number_of_entries(), 0)
        self.assertEqual(len(board.get_ordered_entries()), 0)

    def test_add_entry_board_success(self):
        """
        add_entry adds an entry to the board.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        self.assertEqual(board.get_number_of_entries(), 1)
        self.assertEqual(len(board.get_ordered_entries()), 1)
        self.assertEqual(
            board.get_ordered_entries()[0].id,
            "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_ordered_entries()[0].value, "Create Entry")

    def test_add_entry_board_failure(self):
        """
        add_entry fails when attempting to add an entry, already on the board.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        with self.assertRaises(KeyError):
            board.add_entry(
                "ad32e3f4fb450922ad32e3f4fb450922",
                "second Attempt")
        self.assertEqual(board.get_number_of_entries(), 1)
        self.assertEqual(len(board.get_ordered_entries()), 1)
        self.assertEqual(
            board.get_ordered_entries()[0].id,
            "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(
            board.get_ordered_entries()[0].value,
            "Create Entry")

    def test_update_entry_board_success(self):
        """
        update_entry updates an existing entry to the board.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        board.update_entry("ad32e3f4fb450922ad32e3f4fb450922", "Update Entry")
        self.assertEqual(board.get_number_of_entries(), 1)
        self.assertEqual(len(board.get_ordered_entries()), 1)
        self.assertEqual(
            board.get_ordered_entries()[0].id,
            "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_ordered_entries()[0].value, "Update Entry")

    def test_update_entry_board_failure_on_no_entry(self):
        """
        update_entry fails when attempting to update an empty board.
        """
        board = Board()
        with self.assertRaises(KeyError):
            board.update_entry(
                "ad32e3f4fb450922ad32e3f4fb450922",
                "update attempt")
        self.assertEqual(board.get_number_of_entries(), 0)
        self.assertEqual(len(board.get_ordered_entries()), 0)

    def test_update_entry_board_failure_on_no_other_entry(self):
        """
        update_entry fails when attempting to update an empty not on the board.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        with self.assertRaises(KeyError):
            board.update_entry(
                "e3f4fb450922ad32e3f4fb450922ad32",
                "update Attempt")
        self.assertEqual(board.get_number_of_entries(), 1)
        self.assertEqual(len(board.get_ordered_entries()), 1)
        self.assertEqual(
            board.get_ordered_entries()[0].id,
            "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_ordered_entries()[0].value, "Create Entry")

    def test_delete_entry_board_success(self):
        """
        delete_entry deletes an entry from the board.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        board.delete_entry("ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_number_of_entries(), 0)
        self.assertEqual(len(board.get_ordered_entries()), 0)

    def test_delete_entry_failure_on_empty_board(self):
        """
        delete_entry fails when attempting to delete from an empty board.
        """
        board = Board()
        with self.assertRaises(KeyError):
            board.delete_entry(
                "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_number_of_entries(), 0)
        self.assertEqual(len(board.get_ordered_entries()), 0)

    def test_delete_entry_failure_on_deleted_entry(self):
        """
        delete_entry fails when attempting to delete an already deleted entry.
        """
        board = Board()
        board.add_entry("ad32e3f4fb450922ad32e3f4fb450922", "Create Entry")
        board.delete_entry("ad32e3f4fb450922ad32e3f4fb450922")
        with self.assertRaises(KeyError):
            board.delete_entry(
                "ad32e3f4fb450922ad32e3f4fb450922")
        self.assertEqual(board.get_number_of_entries(), 0)
        self.assertEqual(len(board.get_ordered_entries()), 0)
