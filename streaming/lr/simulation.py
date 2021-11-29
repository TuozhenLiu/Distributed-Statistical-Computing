#! /usr/bin/env python3.7

import numpy as np


n = int(100)
p = 10
x = np.random.randn(n, p)
epsilon = np.random.normal(0, 5, n)
beta = np.arange(1, p+1)
y = np.dot(x, beta) + epsilon
import statsmodels.api as sm
results = sm.OLS(y, x).fit()
#print(beta)
print(results.summary())
#print(results.params)
y = y[:, None]

np.savetxt(fname="data_small.txt", X=np.concatenate([y, x], axis=1))
# print(np.loadtxt("data.csv"))
