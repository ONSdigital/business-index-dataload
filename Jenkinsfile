#!groovy
def server = Artifactory.server 'art-p-01'
def rtGradle = Artifactory.newGradleBuild()
rtGradle.tool = 'gradle_4.9'
rtGradle.resolver server: server, repo: 'ons-repo'
rtGradle.deployer server: server, repo: 'registers-snapshots'
def agentGradleVersion = 'gradle_4-9'

pipeline {
    libraries {
        lib('jenkins-pipeline-shared')
    }
    environment {
        MODULE_NAME = "business-index-dataload"
        GRADLE_OPTS = '-Dorg.gradle.daemon=false'
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
                stash name: 'Checkout'
            }
        }

        stage('Build') {
            agent { label "build.${agentGradleVersion}" }
            steps {
                unstash name: 'Checkout'
                sh 'gradle compileScala'
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

        stage('Validate') {
            failFast true
            parallel {
                stage('Test: Unit') {
                    agent { label "build.${agentGradleVersion}" }
                    steps {
                        unstash name: 'Checkout'
                        sh 'gradle check'
                    }
                    post {
                        success {
                            junit 'build/test-results/test/TEST-uk.gov.ons.*.xml'
                        }
                    }
                }
                stage('Style') {
                    agent { label "build.${agentGradleVersion}" }
                    steps {
                        unstash name: 'Checkout'
                        colourText("info","Running style tests")
                        sh 'gradle scalaStyleMainCheck'
                    }
                    post {
                        success {
                            checkstyle canComputeNew: false, defaultEncoding: '', healthy: '', pattern: 'build/scalastyle/main/scalastyle-check.xml', unHealthy: ''   
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
        
        stage ('Publish') {
            agent { label "build.${agentGradleVersion}" }
            when { 
                branch "master" 
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true 
            }
            steps {
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                unstash name: 'Checkout'
                script {
                    def buildInfo = rtGradle.run tasks: 'clean artifactoryPublish'
                    server.publishBuildInfo buildInfo
                }
                stash name: 'deploy.sh', includes: 'src/main/edge/deploy.sh'
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
        stage ('Deploy: Dev') {
            agent { label 'deploy.jenkins.slave'}
            when { 
                branch "master" 
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true 
            }
            environment{
                DEPLOY_TO = "dev"
                DEPLOY_CRED = "bi-dev-ci-ssh-key"
                VERSION = "0.1.${env.BUILD_NUMBER}"
            }
            steps {
                unstash name: 'deploy.sh'
                sshagent(["${DEPLOY_CRED}"]) {
                    withCredentials([string(credentialsId: "prod1-edgenode-2", variable: 'EDGE_NODE'),
                                    usernamePassword(credentialsId: 'jenk_sbr_prod__artifactory', passwordVariable: 'ART_PWD', usernameVariable: 'ART_USR')]) {
                        sh '''
                            scp -q -o StrictHostKeyChecking=no src/main/edge/deploy.sh bi-${DEPLOY_TO}-ci@${EDGE_NODE}:deploy.sh
                            echo "Successfully copied deploy.sh to HOME directory on ${EDGE_NODE}"
                        '''
                        // Set the environment variables and call deploy.
                        // The `DEPLOY` heredoc doesnt play nice with whitespace hence why there's
                        // no indentation.
                        configFileProvider([configFile(fileId: 'bi_dataload_dev_env_sh', variable: 'BI_DL_DEV_ENV')]) {
                            sh '''
                            source $BI_DL_DEV_ENV
                            ssh -o StrictHostKeyChecking=no bi-${DEPLOY_TO}-ci@${EDGE_NODE} /bin/bash <<-DEPLOY
chmod +x deploy.sh
bash -x deploy.sh $MY_REPO $ARCHIVE $VERSION $ART_URL $ART_USR $ART_PWD $HDFS_USR
DEPLOY'''
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
    }
    post {
        success {
            colourText("success", "All stages complete. Build was successful.")
            slackSend(
                color: "good",
                message: "${currentBuild.fullDisplayName} success: ${env.RUN_DISPLAY_URL}"
            )
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentResult}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            slackSend(
                color: "danger",
                message: "${currentBuild.fullDisplayName} unstable: ${env.RUN_DISPLAY_URL}"
            )
        }
        failure {
            colourText("warn","Process failed at: ${env.NODE_STAGE}")
            slackSend(
                color: "danger",
                message: "${currentBuild.fullDisplayName} failed at ${env.STAGE_NAME}: ${env.RUN_DISPLAY_URL}"
            )
        }
    }
}

def postSuccess() {
    colourText('info', "Stage: ${env.STAGE_NAME} successfull!")
}

def postFail() {
    colourText('warn', "Stage: ${env.STAGE_NAME} failed!")
}