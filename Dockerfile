FROM apache/airflow:2.2.4-python3.6

USER root

RUN apt-get update -y && apt-get install -y wget
RUN wget -O ./mongo-tools.deb https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1804-x86_64-100.5.2.deb
RUN apt-get install ./mongo-tools.deb


# This fixes permission issues on linux. 
# The airflow user should have the same UID as the user running docker on the host system.
ARG DOCKER_UID
RUN \
    : "${DOCKER_UID:?Build argument DOCKER_UID needs to be set and non-empty. Use 'make build' to set it automatically.}" \
    && usermod -u ${DOCKER_UID} airflow \
    && echo "Set airflow's uid to ${DOCKER_UID}"

USER airflow


CMD []



