#!/bin/bash

## the name of this service
SERVICE_NAME=dskit

## need this to resolve symlinks
pushd . > /dev/null
SCRIPT_PATH="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_PATH}" ]) do
  cd "`dirname "${SCRIPT_PATH}"`"
  SCRIPT_PATH="$(readlink "`basename "${SCRIPT_PATH}"`")";
done
cd "`dirname "${SCRIPT_PATH}"`" > /dev/null
SCRIPT_PATH="`pwd`";
popd  > /dev/null

export SERVICE_ROOT="${SCRIPT_PATH}"

## python program
export PYTHON=python

## add app/ directory to PYTHONPATH
export PYTHONPATH=${SERVICE_ROOT}/app

## build virtual python environment
function build_vpython {
        echo "Building in ${SERVICE_ROOT}..."
        cd ${SERVICE_ROOT}
        rm -rf vpython
        virtualenv vpython
        source ${SERVICE_ROOT}/vpython/bin/activate
        pip --version
        pip install -U pip
        pip install -U setuptools
        pip install -U distribute
        echo "Installing requirements.txt..."
        pip install -r requirements.txt
}

## invoke vpython
function activate_vpython {
        export NLTK_DATA=$SERVICE_ROOT/data/nltk_data
        source $SERVICE_ROOT/vpython/bin/activate
}

## download NLTK corpa data
function download_nltk_data {
        python etc/download_nltk_corpa.py
}

SPARK_RELEASE=spark-2.0.2-bin-hadoop2.7

function install_spark {
        mkdir -p ${SERVICE_ROOT}/bin
        pushd ${SERVICE_ROOT}/bin
        curl -o ${SPARK_RELEASE}.tgz http://d3kbcqa49mib13.cloudfront.net/${SPARK_RELEASE}.tgz
        tar xzf ${SPARK_RELEASE}.tgz
        popd
}

case "$1" in
    build_all)
        set -e
        install_spark
        build_vpython
        activate_vpython
        download_nltk_data
        build_collectstatic
        ;;
    build_vpython)
        set -e
        build_vpython
        ;;
    install_requirements)
        set -e
        activate_vpython
        pip install -r requirements.txt
        ;;
    download_nltk_data)
        set -e
        activate_vpython
        download_nltk_data
        ;;
    install_spark)
        set -e
        install_spark
        ;;
    exec)
        activate_vpython
        shift 1
        exec "$@"
	      ;;
    python)
        activate_vpython
        shift 1
        exec ${PYTHON} "$@"
	      ;;
    pg2json)
        activate_vpython
        shift 1
        exec ${PYTHON} app/pg2json.py "$@"
        ;;
    es2json)
        activate_vpython
        shift 1
        exec ${PYTHON} app/es2json.py "$@"
        ;;
    submit)
        export NLTK_DATA=${SERVICE_ROOT}/data/nltk_data
        export PYSPARK_PYTHON=${SERVICE_ROOT}/vpython/bin/python
        export SPARK_SUBMIT=${SERVICE_ROOT}/bin/${SPARK_RELEASE}/bin/spark-submit
        shift 1
        exec ${SPARK_SUBMIT} "$@"
        ;;
esac
