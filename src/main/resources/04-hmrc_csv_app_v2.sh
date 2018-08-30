#!/bin/bash

spark2-submit --class uk.gov.ons.bi.dataload.HmrcBiExportApp --master='yarn' --deploy-mode='cluster' --num-executors 6 --driver-memory 20G --executor-memory 40G --jars hdfs://prod1/${HOME}/businessIndex/lib/config-1.3.2.jar --driver-java-options "-Dbi-dataload.app-data.env=${HOME} -Dbi-dataload.app-data.dir=businessIndex" hdfs://prod1/${HOME}/businessIndex/lib/business-index-dataload_2.11-1.6.jar