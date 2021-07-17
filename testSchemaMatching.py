import unittest

from schemamatching import readFile, compareSchema

class TestSum(unittest.TestCase):
    def ReadFile(self):
        """
            Read the Content of HQL File
        """
        fileLocation = ""
        result = readFile(fileLocation)
        self.assertEqual(result, "Columns table")

    def CompareSchema(self):
        """
            Test to compare the Schema.
        """
        database = ""
        table = ""
        fileLocation = ""
        result = compareSchema(database, table, fileLocation)
        self.assertEqual(result, True)

if __name__ == "__main__":
    unittest.main()