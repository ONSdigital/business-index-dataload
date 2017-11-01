#!groovy
@Library('jenkins-pipeline-shared@feature/version') _

pipeline {
    agent any
    environment {
        LIB_DIR = "ons.gov/businessIndex/$ENV/lib"
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

        stage('Build'){
            agent any
            steps {
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                script {
                    env.NODE_STAGE = "Build"
                    sh '''
                        $SBT clean compile "project api" universal:packageBin coverage test coverageReport
                    '''
                    stash name: 'compiled'
                    if (BRANCH_NAME == BRANCH_DEV) {
                        env.DEPLOY_NAME = DEPLOY_DEV
                    }
                    else if  (BRANCH_NAME == BRANCH_TEST) {
                        env.DEPLOY_NAME = DEPLOY_TEST
                    }
                    else if (BRANCH_NAME == BRANCH_PROD) {
                        env.DEPLOY_NAME = DEPLOY_PROD
                    }
                    else {
                        colourText("info", "Not a deployable Git banch!")
                    }
                }
            }
        }

        stage('Static Analysis') {
            agent any
            steps {
                parallel (
                        "Unit" :  {
                            colourText("info","Running unit tests")
                            // sh "$SBT test"
                        },
                        "Style" : {
                            colourText("info","Running style tests")
                            sh """
                            $SBT scalastyleGenerateConfig
                            $SBT scalastyle
                        """
                        },
                        "Additional" : {
                            colourText("info","Running additional tests")
                            sh "$SBT scapegoat"
                        }
                )
            }
            post {
                always {
                    script {
                        env.NODE_STAGE = "Static Analysis"
                        junit '**/target/test-reports/*.xml'
                    }
                }
                success {
                    colourText("success","Generated reports for tests")
                    //   junit '**/target/test-reports/*.xml'

                    step([$class: 'CoberturaPublisher', coberturaReportFile: '**/target/scala-2.11/coverage-report/*.xml'])
                    step([$class: 'CheckStylePublisher', pattern: 'target/scalastyle-result.xml, target/scala-2.11/scapegoat-report/scapegoat-scalastyle.xml'])
                }
                failure {
                    colourText("warn","Failed to retrieve reports.")
                }
            }
        }

        // bundle all libs and dependencies
        stage ('Bundle') {
            agent any
            when {
                anyOf {
                    branch BRANCH_DEV
                    branch BRANCH_TEST
                    branch BRANCH_PROD
                }
            }
            steps {
                script {
                    env.NODE_STAGE = "Bundle"
                }
//                colourText("info", "Bundling....")
//                dir('conf') {
//                    git(url: "$GITLAB_URL/StatBusReg/${MODULE_NAME}.git", credentialsId: GITLAB_CREDS, branch: "${BRANCH_DEV}")
//                }
                // stash name: "zip"
            }
        }

        stage("Releases"){
            agent any
            when {
                anyOf {
                    branch BRANCH_DEV
                    branch BRANCH_TEST
                    branch BRANCH_PROD
                }
            }
            steps {
                script {
                    env.NODE_STAGE = "Releases"
                    currentTag = getLatestGitTag()
                    colourText("info", "Found latest tag: ${currentTag}")
                    newTag =  IncrementTag( currentTag, RELEASE_TYPE )
                    colourText("info", "Generated new tag: ${newTag}")
                    //push(newTag, currentTag)
                }
            }
        }

        stage ('Package and Push Artifact') {
            agent any
            when {
                branch BRANCH_PROD
            }
            steps {
                script {
                    env.NODE_STAGE = "Package and Push Artifact"
                }
                sh """
                    $SBT clean compile package
                    $SBT clean compile assembly
                """
                colourText("success", 'Package.')
            }
        }

        stage('Deploy'){
            agent any
            when {
                anyOf {
                    branch BRANCH_DEV
                    branch BRANCH_TEST
                    branch BRANCH_PROD
                }
            }
            steps {
                script {
                    env.NODE_STAGE = "Deploy"
                }
                milestone(1)
                lock('Deployment Initiated') {
                    colourText("info", 'deployment in progress')
                    deploy()
                    colourText("success", 'Deploy.')
                }
            }
        }

        stage('Create Index - Dev') {
            steps {
                colourText("info", 'Creating New Index.....')
                createIndex()
                colourText("success", 'Created New Index.')
            }
        }

        stage('Integration Tests') {
            agent any
            when {
                anyOf {
                    branch BRANCH_DEV
                    branch BRANCH_TEST
                }
            }
            steps {
                script {
                    env.NODE_STAGE = "Integration Tests"
                }
                unstash 'compiled'
                sh "$SBT it:test"
                colourText("success", 'Integration Tests - For Release or Dev environment.')
            }
        }
    }
    post {
        always {
            script {
                colourText("info", 'Post steps initiated')
                deleteDir()
            }
        }
        success {
            colourText("success", "All stages complete. Build was successful.")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST"
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentResult}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${env.NODE_STAGE}"
        }
        failure {
            colourText("warn","Process failed at: ${env.NODE_STAGE}")
            sendNotifications currentBuild.result, "\$SBR_EMAIL_LIST", "${env.NODE_STAGE}"
        }
    }
}

def push (String newTag, String currentTag) {
    echo "Pushing tag ${newTag} to Gitlab"
    GitRelease( GIT_CREDS, newTag, currentTag, "${env.BUILD_ID}", "${env.BRANCH_NAME}", GIT_TYPE)
}

def deploy() {
    echo "Deploying to ${env.DEPLOY_NAME}"
    sshagent(credentials: ["${TEAM}-${env.DEPLOY_NAME}-ci-key"]) {
        withCredentials([string(credentialsId: "${TEAM}-${env.DEPLOY_NAME}-host", variable: 'HOST')]) {
            withEnv(["CI_TEAM_ENV = ${TEAM}-${env.DEPLOY_NAME}-ci"]) {
                sh """
                    ssh ${CI_TEAM_ENV}@$HOST mkdir -p $LIB_DIR
                    scp ${WORKSPACE}/runtime/lib/*.jar ${CI_TEAM_ENV}@$HOST:$LIB_DIR
                    scp ${WORKSPACE}/target/*/${MODULE_NAME}*.jar ${CI_TEAM_ENV}@$HOST:$LIB_DIR
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -mkdir -p $LIB_DIR
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -rm -f /$LIB_DIR/*.jar
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -put $LIB_DIR/*.jar /$LIB_DIR
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -put $LIB_DIR/${MODULE_NAME}*.jar /$LIB_DIR
                    ssh ${CI_TEAM_ENV}@$HOST rm -r $LIB_DIR
                    echo "Successfully copied jar files to $LIB_DIR directory on HDFS"
                    ssh ${CI_TEAM_ENV}@$HOST mkdir -p $OOZIE_DIR
                    scp ${WORKSPACE}/src/main/resources/oozie/workflow.xml ${CI_TEAM_ENV}@$HOST:$OOZIE_DIR
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -mkdir -p $OOZIE_DIR
                    ssh ${CI_TEAM_ENV}@$HOST hadoop fs -put -f $OOZIE_DIR/workflow.xml
                    echo "Successfully deployed Oozie job to $OOZIE_DIR directory on HDFS"
                """
            }
        }
    }
}

def createIndex() {
    withCredentials([string(credentialsId: "bi-elastic-url", variable: 'ELASTIC_URL')]) {
        httpRequest httpMode: 'PUT', requestBody: '''{
          "mappings": {
            "business": {
              "properties": {
                "BusinessName": {
                  "type": "string",
                  "boost": 4.0,
                  "analyzer": "bi-devAnalyzer"
                },
                "BusinessName_suggest": {
                  "type": "completion",
                  "analyzer": "simple",
                  "payloads": false,
                  "preserve_separators": true,
                  "preserve_position_increments": true,
                  "max_input_length": 50
                },
                "CompanyNo": {
                  "type": "string",
                  "analyzer": "keyword"
                },
                "EmploymentBands": {
                  "type": "string",
                  "analyzer": "bi-devAnalyzer"
                },
                "IndustryCode": {
                  "type": "long"
                },
                "LegalStatus": {
                  "type": "string",
                  "index": "not_analyzed",
                  "include_in_all": false
                },
                "PayeRefs": {
                  "type": "string",
                  "analyzer": "keyword"
                },
                "PostCode": {
                  "type": "string",
                  "analyzer": "bi-devAnalyzer"
                },
                "TradingStatus": {
                  "type": "string",
                  "index": "not_analyzed",
                  "include_in_all": false
                },
                "Turnover": {
                  "type": "string",
                  "analyzer": "bi-devAnalyzer"
                },
                "UPRN": {
                  "type": "long"
                },
                "VatRefs": {
                  "type": "long"
                }
              }
            }
          },
          "settings": {
            "index": {
              "analysis": {
                "analyzer": {
                  "bi-devAnalyzer": {
                    "filter": [
                      "lowercase"
                    ],
                    "type": "custom",
                    "tokenizer": "whitespace"
                  }
                }
              }
            }
          }
        }''', responseHandle: 'NONE', timeout: 60, url: "$ELASTIC_URL/bi-${env.DEPLOY_NAME}-build-$BUILD_NUMBER", validResponseCodes: '200'
    }
}