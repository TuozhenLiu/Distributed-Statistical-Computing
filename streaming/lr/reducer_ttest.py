#! /usr/bin/env python3.7

import sys
import numpy as np


p = 10
n = 100
SSE = 0

for line in sys.stdin:
	SSE += float(line.strip())

sigma2_hat = SSE / (n-p)

data_0 = np.loadtxt("part-00000")
# data_0 = np.loadtxt("/lifeng/student/liutuozhen/streaming/lr/beta_hat/part-00000")
xTx_inv = data_0[p:].reshape(p, p)
beta_hat =data_0[:p]

beta_se = np.sqrt(np.diagonal(sigma2_hat * xTx_inv))
t = beta_hat / beta_se
print(" ".join([str(i) for i in t]))
