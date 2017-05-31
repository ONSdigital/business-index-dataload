pipeline {
    agent any
    environment {
        ENV = "dev"
        LIB_DIR = "ons.gov/businessIndex/$ENV/lib"
        OOZIE_DIR = "oozie/workspaces/bi-data-ingestion"
    }
    stages {
        stage('Build') {
            steps {
                sh '$SBT clean test package'
            }
        }
        stage('Deploy - Dev') {
            environment {
                ENV = "dev"
            }
            steps {
                deploy()
            }
        }

    }
    post {
        always {
            junit '**/target/test-reports/*.xml'
        }
    }
}

def deploy() {
    echo "Deploying to $ENV"
    sshagent(credentials: ["bi-$ENV-ci-key"]) {
        withCredentials([string(credentialsId: "bi-$ENV-host", variable: 'HOST')]) {
            sh '''
                    ssh bi-$ENV-ci@$HOST mkdir -p $LIB_DIR
                    scp ${WORKSPACE}/runtime/lib/*.jar bi-dev-ci@$HOST:$LIB_DIR
                    scp ${WORKSPACE}/target/*/business-index-dataload*.jar bi-$ENV-ci@$HOST:$LIB_DIR
                    ssh bi-$ENV-ci@$HOST hadoop fs -mkdir -p $LIB_DIR
                    ssh bi-$ENV-ci@$HOST hadoop fs -rm -f /$LIB_DIR/*.jar
                    ssh bi-$ENV-ci@$HOST hadoop fs -put $LIB_DIR/*.jar /$LIB_DIR
                    ssh bi-$ENV-ci@$HOST hadoop fs -put $LIB_DIR/business-index-dataload*.jar /$LIB_DIR
                    ssh bi-$ENV-ci@$HOST rm -r $LIB_DIR
                    echo "Successfully copied jar files to $LIB_DIR directory on HDFS"
                    ssh bi-$ENV-ci@$HOST mkdir -p $OOZIE_DIR
                    scp ${WORKSPACE}/src/main/resources/oozie/workflow.xml bi-$ENV-ci@$HOST:$OOZIE_DIR
                    ssh bi-$ENV-ci@$HOST hadoop fs -mkdir -p $OOZIE_DIR
                    ssh bi-$ENV-ci@$HOST hadoop fs -put -f $OOZIE_DIR/workflow.xml
                    echo "Successfully deployed Oozie job to $OOZIE_DIR directory on HDFS"
                 '''
        }
    }
}