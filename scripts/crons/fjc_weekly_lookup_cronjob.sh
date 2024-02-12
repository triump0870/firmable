#!/bin/bash
# add the following line in the crontab to run this file every Thursday morning
# 0 1 * * 4 /../scripts/crons/fjc_weekly_lookup_cronjob.sh
python extract_fjcs.py