#!/bin/bash

__workflow_name="bi-data-ingestion"
__module_name="business-index-dataload"
__hdfs_install_base="${OOZIE_HOME}/applications/oozie"
__workflow_basedir="${__hdfs_install_base}/${__module_name}-latest/apps/${__workflow_name}"
__workflow_libs="${__workflow_basedir}/lib"

spark2-submit --class uk.gov.ons.bi.dataload.LoadBiToEsApp \
    --master='yarn' \
    --deploy-mode='cluster' \
    --num-executors 6 \
    --driver-memory 4G \
    --executor-memory 3G \
    --jars "hdfs://prod1/${__workflow_libs}/config-1.3.2.jar,hdfs://prod1/${__workflow_libs}/elasticsearch-spark-20_2.11-6.0.0.jar" \
    --driver-java-options \
    "-Dbi-dataload.es.nodes=$1 -Dbi-dataload.es.index=$2 -Dbi-dataload.app-data.env=${OOZIE_HOME} -Dbi-dataload.app-data.dir=businessIndex" \
    hdfs://prod1/${__workflow_libs}/business-index-dataload-*.jar