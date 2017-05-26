pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
        sh '$SBT clean test package'
      }
    }
    stage('HDFS Upload') {
      steps {
        sh '''sshagent(credentials: [$DEPLOY_USER]) {
  LIB_DIR = /index/business/ingestion/lib
  HDFS_DIR = /ons.gov/businessIndex/$ENV/lib
  ssh mkdir -p $LIB_DIR
  ssh cp $WORKSPACE/**/*.jar LIB_DIR
  ssh hadoop fs -rm $HDFS_DIR
  ssh hadoop fs -put -f LIB_DIR/*.jar $HDFS_DIR
  ssh rm -r LIB_DIR
}'''
        }
      }
      stage('Deploy - Dev') {
        steps {
          sh '''sshagent(credentials: [$DEPLOY_USER]) {
  OOZIE_DIR = /oozie/workspaces/bi-data-ingestion
  hadoop fs -mkdir -p $OOZIE_DIR
  hadoop fs -put -f $OOZIE_DIR/workflow.xml
}'''
          }
        }
      }
      environment {
        ENV = 'dev'
        DEPLOY_USER = 'bi-dev-ci'
      }
    }