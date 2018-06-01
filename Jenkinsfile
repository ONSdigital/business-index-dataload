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

        CF_CREDS = "sbr-api-dev-secret-key"

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