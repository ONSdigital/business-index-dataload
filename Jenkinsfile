#!groovy
@Library('jenkins-pipeline-shared@master') _


pipeline {
    environment {
        OOZIE_DIR = "oozie/workspaces/bi-data-ingestion"

        RELEASE_TYPE = "PATCH"

        BRANCH_DEV = "develop"
        BRANCH_TEST = "release"
        BRANCH_PROD = "master"

        DEPLOY_DEV = "dev"
        DEPLOY_TEST = "test"
        DEPLOY_PROD = "prod"

        GIT_TYPE = "Github"
        GIT_CREDS = "github-sbr-user"
        GITLAB_CREDS = "sbr-gitlab-id"

        ORGANIZATION = "ons"
        TEAM = "bi"
        MODULE_NAME = "business-index-dataload"
    }
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 30, unit: 'MINUTES')
        timestamps()
    }
    agent any
    stages {
        stage('Checkout'){
            agent any
            steps{
                deleteDir()
                checkout scm
                stash name: 'app'
                sh "$SBT version"
                script {
                    version = '1.0.' + env.BUILD_NUMBER
                    currentBuild.displayName = version
                    env.NODE_STAGE = "Checkout"
                }
            }
        }

        stage('Static Analysis') {
            agent any
            steps {
                sh "sbt test"
            }
        }

        stage ('Package and Push Artifact') {
            agent any
            steps {
                sh "sbt package"
                copyToEdgeNode()
            }
        }
    }
}

def copyToEdgeNode() {
    echo "Deploying to $DEPLOY_DEV"
    sshagent(credentials: ["bi-dev-ci-ssh-key"]) {
        withCredentials([string(credentialsId: "prod1-edgenode-2", variable: 'EDGE_NODE'),
                         string(credentialsId: "hdfs-jar-path-dev", variable: 'JAR_PATH')]) {
            sh '''
                ssh bi-$DEPLOY_DEV-ci@$EDGE_NODE mkdir -p $MODULE_NAME/lib
                scp ${WORKSPACE}/target/scala-*/business-index-dataload*.jar bi-$DEPLOY_DEV-ci@$EDGE_NODE:$MODULE_NAME/lib/
                echo "Successfully copied jar file to $MODULE_NAME/lib directory on $EDGE_NODE"
                ssh bi-$DEPLOY_DEV-ci@$EDGE_NODE hdfs dfs -put -f $MODULE_NAME/lib/business-index-dataload_2.11-1.5.jar $JAR_PATH
                echo "Successfully copied jar file to HDFS"
	        '''
        }
    }
}
