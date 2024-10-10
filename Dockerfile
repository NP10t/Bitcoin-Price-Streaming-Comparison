FROM apache/airflow:2.6.0-python3.9

# Sao chép các tệp cần thiết vào container
COPY --chown=airflow:root ./script/entrypoint.sh /opt/airflow/script/entrypoint.sh
COPY ./requirements.txt /opt/airflow/requirements.txt

# Cài đặt các yêu cầu từ requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Đảm bảo rằng entrypoint.sh có quyền thực thi
USER root
RUN chmod +x /opt/airflow/script/entrypoint.sh
USER airflow