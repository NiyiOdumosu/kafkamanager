pipeline {
    agent any

    stages {
        stage('execute') {
            
            steps {
               script {
                
                    
                    env.TAG = sh (script: "printf \$(git rev-parse HEAD)", returnStdout: true)     
                    echo "this is the git revision "+env.TAG
                    env.PREVTAG = sh (script: "printf \$(git rev-parse HEAD~1)", returnStdout: true)

                    env.gitdiff = sh (script: "git diff --name-status $env.PREVTAG $env.TAG", returnStdout: true)
                    echo env.gitdiff
                    env.connectorURL="http://localhost:8083/connectors/"

                    env.restURL="http://localhost:8082/"
                    env.kafkaClusterID="di3r55ecSeij5yE31X7xnA"

                    env.kafka_user="rahul"

                    git show HEAD:application1/topics/topics.json > current.json
                    git show HEAD^:application1/topics/topics.json > previous.json

                    sh ('python3 pipeline.py application1/acls/acls.json  $SOURCE_BRANCH application1/acls/acls.json $TARGET_BRANCH')

                   
                }
            }
            }
        }
    
}
