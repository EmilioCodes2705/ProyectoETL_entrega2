FROM apache/airflow:2.5.1   

RUN pip install --no-cache-dir \
    sodapy \
    pandas \
    pyarrow \
    SQLAlchemy \
    psycopg2-binary \
    python-dotenv