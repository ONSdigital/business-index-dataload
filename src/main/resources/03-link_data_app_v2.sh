#!/bin/bash

spark2-submit --class uk.gov.ons.bi.dataload.LinkDataApp --master='yarn' --deploy-mode='cluster' --num-executors 6 --driver-memory 4G --executor-memory 3G --jars hdfs://prod1/${HOME}/businessIndex/lib/config-1.3.2.jar --driver-java-options "-DBI-DATALOAD_ENV=${OOZIE_HOME} -DBI_DATALOAD_CLUSTER=cluster" hdfs://prod1/${HOME}/businessIndex/lib/business-index-dataload_2.11-1.6.jar