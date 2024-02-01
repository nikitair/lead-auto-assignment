#!/bin/bash
source /home/olegl/retool_processor/lead_auto_assignment_automation/.venv/bin/activate
cd /home/olegl/retool_processor/lead_auto_assignment_automation/src
daphne server:app -p 5050 -b 172.31.26.132
