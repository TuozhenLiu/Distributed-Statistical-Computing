#! /bin/bash

# print some columns of my data
awk -F ' ' '$2=="male"{printf ("%s\t%s\n", $1, $3)}'
