//Jenkins file for dataservice-client-model

pipeline {
    agent any
    stages {
		stage('Clean') {
            steps {
                sh "chmod +x ./gradlew"
                sh "./gradlew clean"
            }
        }
        
		stage('Compile') {
            steps {
                sh "./gradlew compileJava"
            }
        }
		
        stage('Test') {
            steps {
                script {
                    try {
                        sh "./gradlew test"
                    } finally {
                        publishHTML(target: [allowMissing         : true,
                                             alwaysLinkToLastBuild: true,
                                             keepAll              : true,
                                             reportDir            : 'build/reports/tests/test',
                                             reportFiles          : 'index.html',
                                             reportName           : 'Test Results',
                                             reportTitles         : '']
                        )
                    }
                }
            }
        }

        stage('Jar') {
            steps {
                sh "./gradlew jar"
            }
        }

        stage('Nexus Upload') {
            environment {
                JENKINS_USER_CREDS = credentials('jenkins-ldap-user-password')
                ENV_NAME = "\${env.BRANCH_NAME}"
            }
            when {
                anyof{
                    branch 'develop';
                    branch 'master';
                    tag '*'
                }
            }
            steps {
                echo 'Publishing Branch: ' + ENV_NAME
                sh "./gradlew publish -PuploadRepoUsername=\$JENKINS_USER_CREDS_USR -PuploadRepoPassword=\$JENKINS_USER_CREDS_PSW"
            }
        }
    }
}
