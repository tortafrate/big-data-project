#!/bin/bash

OUTPUT_FILE="rapport_debug.txt"

echo "--- Rapport de débogage du $(date) ---" > "$OUTPUT_FILE"

log_command() {
    CMD="$1"
    echo "Exécution de : $CMD"
    echo -e "\n>>> $CMD <<<" >> "$OUTPUT_FILE"
    eval "$CMD" >> "$OUTPUT_FILE" 2>&1
    echo -e "\n---------------------------------------------------\n" >> "$OUTPUT_FILE"
}

log_command "docker compose ps -a"
log_command "docker stats --no-stream"

log_command "docker compose logs elasticsearch"
log_command "docker compose logs minio"
log_command "docker compose logs spark-master"
log_command "docker compose logs postgres"
log_command "docker compose logs spark-worker"
log_command "docker compose logs kibana"
log_command "docker compose logs airflow-init"
log_command "docker compose logs airflow-scheduler"
log_command "docker compose logs airflow-api-server"
log_command "docker compose logs airflow-dag-processor"

log_command "docker compose logs minio-create-bucket"

log_command "docker compose logs spark-master | grep -i 'Registering worker' || echo 'Aucun worker enregistré pour le moment'"

log_command "docker compose exec -T airflow-scheduler airflow dags list"
log_command "docker compose exec -T airflow-scheduler airflow db check"

log_command "curl -s -X GET 'http://localhost:9200/_cluster/health?pretty' || echo 'Impossible de joindre Elasticsearch sur le port 9200'"

log_command "docker exec -it airflow_scheduler airflow config get-value core execution_api_server_url"
log_command "docker exec -it airflow_scheduler curl -sf http://airflow-api-server:8080/execution/ || echo 'KO'"

echo "Terminé. Résultats complets dans $OUTPUT_FILE"