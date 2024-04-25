#! /bin/sh
python3 /home/ubuntu/airflow/dags/govhub_ms_sg_upload/src/project_upload_process.py

retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Error"
fi
exit $retVal
