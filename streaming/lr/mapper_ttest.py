#! /usr/bin/env python3.7

import sys
import numpy as np


p = 10
data = np.loadtxt(sys.stdin)

beta = np.loadtxt("part-00000")
# beta = np.loadtxt("/lifeng/student/liutuozhen/streaming/lr/beta_hat/part-00000")
beta = beta[:p]

x = data[:, 1:]
y = data[:, 0]

e = np.dot(x, beta) - y
SSE = np.inner(e, e)
print(SSE)
