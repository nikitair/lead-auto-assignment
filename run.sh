#!/bin/bash
source /home/olegl/retool_processor/lead_auto_assignment_automation/.venv/bin/activate
cd /home/olegl/retool_processor/lead_auto_assignment_automation
daphne server:app -p 5000 -b 172.31.26.132
