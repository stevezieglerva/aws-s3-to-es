import unittest
from lambda_function import *
import time


class TestMethods(unittest.TestCase):
	def test_lambda_handler__bad_event__error_message_returned(self):
		# Arrange

		# Act
		result = lambda_handler({"hello" : "world"}, "testing")

		# Assert
		self.assertEqual(result["msg"], "Error: No key 'Records' in the event")


	def test_get_index_from_path__valid_path_with_dir__dir_returned(self):
		# Arrange
		input = "https://s3.amazonaws.com/aws-s3-to-es/index_name_dir/test.txt"

		# Act
		result = get_index_from_path(input)

		# Assert
		self.assertEqual("index_name_dir", result)

	def test_get_index_from_path__path_without_dir__returns_general(self):
		# Arrange
		input = "https://s3.amazonaws.com/aws-s3-to-es/test.txt"

		# Act
		result = get_index_from_path(input)

		# Assert
		self.assertEqual("general", result)		

	def test_get_index_from_path__empty_string__throws_exception(self):
		# Arrange

		# Act

		# Assert
		self.assertRaises(ValueError, get_index_from_path, "")


if __name__ == '__main__':
	unittest.main()		


