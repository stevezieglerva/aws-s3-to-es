import unittest
from lambda_function import *
import time


class TestMethods(unittest.TestCase):
	def test_get_index_from_path__empty_path__return_empty_string(self):
		# Arrange

		# Act
		index = get_index_from_path("")

		# Assert
		self.assertEqual(index, "")

	def test_lambda_handler__bad_event__error_message_returned(self):
		# Arrange

		# Act
		result = lambda_handler({"hello" : "world"}, "testing")

		# Assert
		self.assertEqual(result, "Error: No key 'Records' in the event")



if __name__ == '__main__':
	unittest.main()		


