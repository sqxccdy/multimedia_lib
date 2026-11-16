import sys

from adp_operator import serve
from adp_operator.c2c.operator import OperatorABC


import numpy as np
import pandas as pd
class TestNode(OperatorABC):
    @classmethod
    def get_desc(lang: str = "zh"):
        return (
            "TestNode"
        )
    def run(self, storage) -> None:
        pass


if __name__ == '__main__':
    serve(TestNode, sys.argv)
