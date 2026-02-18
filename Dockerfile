FROM apache/airflow:2.7.1

RUN pip install --no-cache-dir \
    requests==2.31.0 \
    pandas==2.0.3 \
    pyarrow==14.0.1 \
    python-dotenv==1.0.0