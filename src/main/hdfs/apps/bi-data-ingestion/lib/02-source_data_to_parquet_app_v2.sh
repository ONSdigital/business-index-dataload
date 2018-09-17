#!/bin/bash

__workflow_name="bi-data-ingestion"
__module_name="business-index-dataload"
__hdfs_install_base="${OOZIE_HOME}/applications/oozie"
__workflow_basedir="${__hdfs_install_base}/${__module_name}-latest/apps/${__workflow_name}"
__workflow_libs="${__workflow_basedir}/lib"

spark2-submit --class uk.gov.ons.bi.dataload.SourceDataToParquetApp \
    --master='yarn' \
    --deploy-mode='cluster' \
    --num-executors 6 \
    --driver-memory 2G \
    --executor-memory 4G \
    --jars "hdfs://prod1/${__workflow_libs}/config-1.3.2.jar" \
    --driver-java-options \
    "-Dbi-dataload.ext-data.env=${OOZIE_HOME} -Dbi-dataload.ons-data.dir=bi-dev-ci/businessIndex -Dbi-dataload.app-data.env=user -Dbi-dataload.app-data.dir=bi-dev-ci/businessIndex" \
    hdfs://prod1/${__workflow_libs}/business-index-dataload-*.jar