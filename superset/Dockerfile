FROM apache/superset:latest

USER root

RUN pip install --upgrade pip
RUN pip install psycopg2-binary

COPY ./superset-init.sh /superset-init.sh
RUN chmod +x /superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

USER superset
ENTRYPOINT ["/bin/bash", "/superset-init.sh"]
