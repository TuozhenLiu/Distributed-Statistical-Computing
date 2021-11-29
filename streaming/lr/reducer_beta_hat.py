#! /usr/bin/env python3.7

import sys
import numpy as np


p = 10
data = np.loadtxt(sys.stdin)
if data.ndim == 2:
    data = data.sum(axis=0)
data = data.reshape(p, -1)

xTx = data[:, 1:]
xTy = data[:, 0]

# print(xTx.shape, xTy.shape)

beta = np.linalg.solve(xTx, xTy)  # (p, )
xTx_inv = np.linalg.inv(xTx) # (p, p)
beta = beta[None, :]
beta_xTx_inv = np.concatenate([beta, xTx_inv], axis=0).reshape(-1)
print(" ".join([str(i) for i in beta_xTx_inv]))
