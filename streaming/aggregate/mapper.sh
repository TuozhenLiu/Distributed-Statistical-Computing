#! /bin/bash

# print some columns of my data
awk -F ',' '{printf ("%s\t%s\n", $1, $3)}'
