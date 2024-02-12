#!/bin/bash
# add the following line in the crontab to run this file every Thursday morning
# 0 2 * * 4 /../scripts/crons/afsl_weekly_lookup_cronjob.sh
python extract_asic_afsl.py