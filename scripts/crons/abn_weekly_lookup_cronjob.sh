#!/bin/bash
# add the following line in the crontab to run this file every Thursday morning
# 0 0 * * 4 /../scripts/crons/abn_weekly_lookup_cronjob.sh
python extract_abns.py
