#! /bin/bash

# print some columns of my data
awk -F ',' '{printf ("%s\t%s\t%s\t%s\n", $1, $2, $3, $6)}'
