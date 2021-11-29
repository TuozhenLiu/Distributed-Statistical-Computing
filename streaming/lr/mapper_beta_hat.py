#! /usr/bin/env python3.7

import sys
import numpy as np


data = np.loadtxt(sys.stdin)
# print(data)

x = data[:, 1:]

xTy_xTx = np.dot(x.T, data).reshape(-1)

print(" ".join(str(i) for i in xTy_xTx))
