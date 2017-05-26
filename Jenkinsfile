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
          withCredentials([
                  [$class: 'UsernamePasswordMultiBinding', credentialsId: 'bi-test-ci', usernameVariable: 'CLOUDERA_ACCESS_USR', passwordVariable: 'CLOUDERA_ACCESS_PWD'],
                  [$class: 'StringBinding', credentialsId: 'bi-test-host', variable: 'CLOUDERA_HOST']]) {
            sh "ssh $CLOUDERA_ACCESS_USR@$CLOUDERA_HOST"
            sh 'expect "password"'
            sh "send $CLOUDERA_ACCESS_PWD\r"
            sh 'interact'
            sh 'LIB_DIR = /index/business/ingestion/lib'
            sh 'HDFS_DIR = /ons.gov/businessIndex/test/lib'
            sh 'ssh mkdir -p $LIB_DIR'
            sh 'ssh cp $WORKSPACE/**/*.jar LIB_DIR'
            sh 'ssh hadoop fs -rm $HDFS_DIR'
            sh 'ssh hadoop fs -put -f LIB_DIR/*.jar $HDFS_DIR'
            sh 'ssh rm -r LIB_DIR'
          }
        }
    }
    stage('Deploy Oozie Job') {
      steps {
        sh 'OOZIE_DIR = /oozie/workspaces/bi-data-ingestion'
        sh 'ssh mkdir -p OOZIE_DIR'
        sh 'ssh cp $WORKSPACE/src/main/resources/oozie/workflow.xml LIB_DIR'
        sh 'hadoop fs -mkdir -p $OOZIE_DIR'
        sh 'hadoop fs -put -f $OOZIE_DIR/workflow.xml'
      }
    }
  }
  post {
    always {
      junit '**/target/test-reports/*.xml'
    }
  }
}