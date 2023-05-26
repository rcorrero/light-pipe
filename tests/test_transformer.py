__author__ = "Richard Correro (richard@richardcorrero.com)"


import unittest
from typing import List

from light_pipe import BlockingThreadPooler, Data, make_data, make_transformer


class TestTransformers(unittest.TestCase):
    @staticmethod
    @make_data
    def gen_tups(x: int):
        for i in range(x):
            yield (3 * i, 3 * i + 1, 3 * i + 2)


    @staticmethod
    @make_data
    def gen_dicts(x: int):
        for i in range(x):
            yield {
                "one": 3 * i, 
                "two": 3 * i + 1, 
                "three": 3 * i + 2
            }
    

    @staticmethod
    @make_transformer
    def get_third(one: int, two: int, three: int):
        return three
    

    def test_tuple_to_args(self):
        gen_data: Data = self.gen_tups() >> self.get_third()
        results: List[int] = gen_data(x=3, block=True)
        self.assertEqual(results, [2,5,8])


    def test_dict_to_kwargs(self):
        gen_data: Data = self.gen_dicts() >> self.get_third(
            parallelizer=BlockingThreadPooler(max_workers=2, queue_size=10)
        )
        results: List[int] = gen_data(x=3, block=True)
        # self.assertEqual(results, [2,5,8])
        for result in results:
            self.assertIn(result, [2,5,8])
    

if __name__ == "__main__":
    unittest.main()
