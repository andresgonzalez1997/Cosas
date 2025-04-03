import sys
import os
if '/home/cdsw/' not in sys.path: sys.path.append('/home/cdsw/')
os.chdir('/home/cdsw/')

import unittest
import purina_file as pur

class TestPurinaFile(unittest.TestCase):
    
    def test_read_file(self):
        test_file = "tests/support_files/test_file.pdf"
        result = pur.read_file(test_file)
        self.assertTrue(len(result) > 1)
    
    def test_read_file_with_invalid_filepath(self):
        test_file = "tests/support_files/invalid_path.pdf"
        result = pur.read_file(test_file)
        self.assertFalse(result)
        
    def test_price_list(self):
        test_file = "tests/support_files/test_file.pdf"
        table_list = pur.read_file(test_file)
        price_list = pur.price_list(table_list)
        self.assertTrue(price_list.iloc[1,0], "1760236-406")
        
    
    
    
if __name__ == "__main__":
    unittest.main()