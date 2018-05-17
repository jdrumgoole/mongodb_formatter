import unittest

from pymongo_formatter.nested_dict import DotDict

class Test( unittest.TestCase):

    def test_DotDict(self):


        x = DotDict()
        c={ "c":"e"}
        b = { "b" : c}
        x[ "a"] =b
        print( x )
        print( x["a"] )
        print( x["a.b"] )
        print( x["a.b.c"] )
        x["a.b.c"] = "f"
        print( x["a.b.c"] )
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()