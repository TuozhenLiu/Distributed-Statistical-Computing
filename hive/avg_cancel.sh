#! /bin/bash

hive -f avg_cancel.sql &>avg_cancel.log

cat avg_cancel.log
