#!groovy
def artServer = Artifactory.server 'art-p-01'
def buildInfo = Artifactory.newBuildInfo()
def distDir = 'build/dist/'

pipeline {
    libraries {
        lib('jenkins-pipeline-shared')
    }
    environment {
        MODULE_NAME = "business-index-dataload"
    }
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 1, unit: 'HOURS')
        ansiColor('xterm')
    }
    agent { label 'download.jenkins.slave'}
    stages {
        stage('Checkout'){
            agent { label 'download.jenkins.slave'}
            steps{
                deleteDir()
                checkout scm
               colourText('info', "Buildinfo: ${buildInfo.name}-${buildInfo.number}")
               stash name: 'Checkout'
            }
        }

        stage('Build') {
            agent { label 'build.sbt_0-13-13' }
            steps {
                unstash name: 'Checkout'
                sh 'sbt compile'
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }
        stage('Test: Unit') {
            agent { label 'build.sbt_0-13-13' }
            steps {
                unstash name: 'Checkout'
                sh 'sbt coverage test coverageReport coverageAggregate'
            }
            post {
                always {
                    junit '**/target/test-reports/*.xml'
                    cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'target/scala-*/coverage-report/cobertura.xml', conditionalCoverageTargets: '70, 0, 0', failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', onlyStable: false, zoomCoverageChart: false
                }
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage('Static Analysis') {
            failFast true
            parallel {
                stage('Style') {
                    agent { label 'build.sbt_0-13-13' }
                    steps {
                        unstash name: 'Checkout'
                        colourText("info","Running style tests")
                        sh 'sbt scalastyleGenerateConfig scalastyle'
                    }
                    post {
                        success {
                            checkstyle canComputeNew: false, defaultEncoding: '', healthy: '', pattern: 'target/scalastyle-result.xml', unHealthy: ''
                        }
                    }
                }
                stage('Additional') {
                    agent { label 'build.sbt_0-13-13' }
                    steps {
                        unstash name: 'Checkout'
                        colourText("info","Running additional tests")
                        sh 'sbt scapegoat'
                    }
                    post {
                        success {
                            checkstyle canComputeNew: false, defaultEncoding: '', healthy: '', pattern: 'target/scala-2.11/scapegoat-report/scapegoat-scalastyle.xml', unHealthy: ''
                        }
                    }
                }
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }
        // stage ('Package and Push Artifact') {
        //     agent any
        //     steps {
        //         sh "sbt package"
        //         copyToEdgeNode()
        //     }
        // }
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
                ssh bi-$DEPLOY_DEV-ci@$EDGE_NODE hdfs dfs -put -f $MODULE_NAME/lib/business-index-dataload_2.11-1.6.jar $JAR_PATH
                echo "Successfully copied jar file to HDFS"
	        '''
        }
    }
}

def postSuccess() {
    colourText('info', "Stage: ${env.STAGE_NAME} successfull!")
}

def postFail() {
    colourText('warn', "Stage: ${env.STAGE_NAME} failed!")
}