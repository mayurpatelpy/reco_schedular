pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/mayurpatelpy/reco_schedular.git'  // Your GitHub repo URL
        DOCKER_IMAGE = 'reco_schedular'  // Docker image name
        GIT_CREDENTIALS = 'git_cred'  // Jenkins GitHub credentials ID
    }

    stages {
        stage('Clone Repository') {
            steps {
                // Pull the code from the private GitHub repository
                git branch: 'main',
                    credentialsId: "${env.GIT_CREDENTIALS}",
                    url: "${env.REPO_URL}"
            }
        }

        stage('Remove Old Code') {
            steps {
                script {
                    sh '''
                    rm -r /home/projects/reco_schedular/*
                    '''
                }
            }
        }

        stage('Copy New Code') {
            steps {
                script {
                    sh '''
                    cp -r * /home/projects/reco_schedular/
                    cd /home/projects/reco_schedular/
                    '''
                }
            }
        }

        stage('Run Docker Compose') {
            steps {
                script {
                    sh '''
                    cd /home/projects/reco_schedular/
                    docker-compose down || true
                    docker-compose build --no-cache && docker-compose up -d
                    '''
                }
            }
        }
    }

    post {
        always {
            // Clean up workspace after build
            cleanWs()
        }
    }
}
