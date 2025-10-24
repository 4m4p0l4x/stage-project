pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Integrator') {
            steps {
                sh 'echo "Ejecutando CASA pipeline..."'
                sh 'casa -c monitor_casa_pipeline.py'
            }
        }

        stage('Export Metrics') {
            steps {
                sh 'curl -s http://localhost:9091/metrics | tee casa_metrics.prom || true'
            }
        }
    }
}
