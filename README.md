# Airflow-Data-Ingestion
Simple Load of a CSV/XLX file into Postgres using Airflow orchestration


# Building a virtual environment:
mkdir datapipeline
cd datapipeline
python3 -m venv .env
source .env/bin/activate

Criaremos uma variável de ambiente que aponta para a pasta onde o
Airflow será instalado

export AIRFLOW_HOME=$(pwd)/airflow

# -- INSTALL AIRFLOW
pip install "apache-airflow[celery]==2.2.4" --constraint
"https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/${PYTHON_VERSION}.txt"


# Initiate the metadata database of airflow:
airflow db init

# Start Webserver services:
airflow webserver

# Start Scheduler services:
airflow scheduler

#####################################
# Start locally airflow
airflow standalone
#####################################

# create a user

airflow users create --username <username> --firstname <first>
--lastname <last> --role Admin --email email@.co.com

########################################################################################################

SqlAchemy error: When running a airflow dag, there is a probability of the following error occur:

[2022-04-01, 14:58:35 -03] {subprocess.py:89} INFO - Unable to find a usable engine; tried using: 'sqlalchemy'.
[2022-04-01, 14:58:35 -03] {subprocess.py:89} INFO - A suitable version of sqlalchemy is required for sql I/O support.
[2022-04-01, 14:58:35 -03] {subprocess.py:89} INFO - Trying to import the above resulted in these errors:
[2022-04-01, 14:58:35 -03] {subprocess.py:89} INFO -  - Pandas requires version '1.4.0' or newer of 'sqlalchemy' (version '1.3.24' currently installed).

HOW TO FIX THE ERROR ABOVE

##########################################################################################################

# Installing Airflow with extras and providers
# If you need to install extra dependencies of airflow, you can use the script below to make an installation a one-liner (the example below installs postgres and google provider, as well as async extra.
##########################################################################################################

AIRFLOW_VERSION=2.2.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[async,postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
