#!/bin/bash
source /home/mysql/.bash_profile
cd /data01/lianjia/lianjia-beike-spider && source venv/bin/activate
python3 ershou.py wh >> logs/ershou_`date +%Y%m%d`.log 2>&1
python3 ershou.py dg >> logs/ershou_`date +%Y%m%d`.log 2>&1
python3 fangjia_analyze.py >> logs/fangjia_analyze.log 2>&1

