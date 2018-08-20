#!/bin/bash​

spark2-submit --class uk.gov.ons.bi.dataload.PreprocessLinksApp --master='yarn' --deploy-mode='cluster' --num-executors 6 --driver-memory 20G --executor-memory 40G --jars hdfs://prod1/user/bi-dev-ci/businessIndex/lib/config-1.3.2.jar --driver-java-options "-Dbi-dataload.app-data.env=user/bi-dev-ci -Dbi-dataload.app-data.dir=businessIndex -Dbi-dataload.app-data.work=user/bi-dev-ci/businessIndex/WORKINGDATA -Dbi-dataload.app-data.links=LINKS_Output.parquet -Dbi-dataload.ons-data.links.parquet=user/bi-dev-ci/bi-parquet/legal_units.parquet" hdfs://prod1/user/bi-dev-ci/businessIndex/lib/business-index-dataload_2.11-1.6.jar