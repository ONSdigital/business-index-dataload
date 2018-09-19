#!/bin/bash​

__workflow_name="bi-data-ingestion"
__module_name="business-index-dataload"
__hdfs_install_base="${OOZIE_HOME}/applications/oozie"
__workflow_basedir="${__hdfs_install_base}/${__module_name}-latest/apps/${__workflow_name}"
__workflow_libs="${__workflow_basedir}/lib"

spark2-submit --class uk.gov.ons.bi.dataload.PreprocessLinksApp \
    --master='yarn' \
    --deploy-mode='cluster' \
    --num-executors 6 \
    --driver-memory 20G \
    --executor-memory 40G \
    --jars "hdfs://prod1/${__workflow_libs}/config-1.3.2.jar" \
    --driver-java-options \
    "-DBI_DATALOAD_LINKS_DATA_DIR=${OOZIE_HOME}/bi-parquet -DBI_DATALOAD_APP_DATA_WORK=${OOZIE_HOME}/businessIndex/WORKINGDATA -Dbi-dataload.app-data.cluster=cluster" \
    hdfs://prod1/${__workflow_libs}/business-index-dataload-*.jar