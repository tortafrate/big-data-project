# Big Data Project - Pipeline d'Ingestion Airsoft

Ce projet déploie une architecture Big Data complète et conteneurisée en local pour **scraper, traiter, analyser et indexer des annonces de vente d'Airsoft**. Il intègre un Data Lake (MinIO), un cluster de traitement distribué (Spark), un orchestrateur de workflows (Airflow) et une stack de visualisation (Elasticsearch & Kibana).

## 🏗 Architecture du Pipeline

Le pipeline de données est orchestré par le DAG Airflow `1_ingestion_complete_pipeline` et suit une architecture en couches (Bronze/Silver/Gold) :

1.  **Ingestion (Bronze / Raw)** :
    * **Source** : Scrape les annonces depuis [airsoft-occasion.fr](https://www.airsoft-occasion.fr).
    * **Techno** : Script Python (`scraper.py`) avec `BeautifulSoup` et `boto3`.
    * **Stockage** : JSON brut dans MinIO (`s3://datalake/raw/...`).

2.  **Nettoyage (Silver / Formatted)** :
    * **Traitement** : Job Spark (`spark_cleaning.py`) qui lit les JSON bruts, extrait les prix, nettoie les titres et normalise les schémas.
    * **Stockage** : Parquet partitionné par date dans MinIO (`s3://datalake/formatted/...`).

3.  **Agrégation (Gold / Usage)** :
    * **Traitement** : Job Spark (`spark_aggregation.py`) qui calcule des statistiques journalières (prix moyen, min, max, nombre d'annonces).
    * **Stockage** : Parquet dans MinIO (`s3://datalake/usage/...`).

4.  **Indexation & Visualisation** :
    * **Indexation** : Script Python (`indexer.py`) qui lit la couche *Formatted* et pousse les documents dans **Elasticsearch** (index `airsoft-ads`).
    * **Visualisation** : Exploration des données via **Kibana**.

## 📋 Prérequis

* **Docker** et **Docker Compose** (V2 recommandée).
* **Git**.
* Minimum **6-8 Go de RAM** alloués à Docker (la stack complète est gourmande).
* **OpenSSL** (généralement préinstallé sur Linux/macOS/WSL) pour générer les clés.

## 🚀 Installation et Démarrage

1.  **Cloner le projet** :
    ```bash
    git clone [https://github.com/tortafrate/big-data-project.git](https://github.com/tortafrate/big-data-project.git)
    cd big-data-project-agdb__scraping_-_ingestion
    ```

2.  **Configuration de l'environnement** :
    Copiez le fichier d'exemple pour initialiser les variables :
    ```bash
    cp .env.exemple .env
    ```

    **⚠️ Génération des clés de sécurité (Obligatoire)** :
    Le fichier `.env` contient des variables critiques (`AIRFLOW__WEBSERVER__SECRET_KEY` et `AIRFLOW__API_AUTH__JWT_SECRET`) marquées comme `<à initialiser>`. Vous devez les remplacer par des chaînes aléatoires sécurisées.

    Exécutez cette commande pour générer une clé hexadécimale :
    ```bash
    openssl rand -hex 32
    ```
    Copiez la sortie et remplacez la valeur dans `.env`. Répétez l'opération pour la deuxième clé.
    
    *Alternative rapide (Linux/Mac/WSL) pour tout remplacer automatiquement :*
    ```bash
    sed -i "s/<à initialiser>/$(openssl rand -hex 32)/g" .env
    ```

3.  **Lancement des services** :
    Construisez les images (Airflow custom, Spark) et démarrez les conteneurs :
    ```bash
    docker compose up -d --build
    ```

## 🖥️ Accès aux Interfaces

Une fois l'infrastructure démarrée, les services sont accessibles aux adresses suivantes :

| Service | URL | Identifiants par défaut (dans .env) |
| :--- | :--- | :--- |
| **Airflow** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Spark Master** | [http://localhost:9090](http://localhost:9090) | *(Aucun)* |
| **Kibana** | [http://localhost:5601](http://localhost:5601) | *(Aucun)* |

## ▶️ Utilisation

1.  Rendez-vous sur l'interface **Airflow**.
2.  Activez le DAG **`1_ingestion_complete_pipeline`**.
3.  Déclenchez-le manuellement (bouton "Trigger DAG") ou attendez l'exécution planifiée (`@daily`).

**Vérification des données :**
* Dans **MinIO** : Naviguez dans le bucket `datalake` pour voir les dossiers `raw`, `formatted` et `usage`.
* Dans **Kibana** :
    * Allez dans *Stack Management* > *Data Views*.
    * Créez une vue pour l'index `airsoft-ads`.
    * Visualisez les annonces dans *Discover*.

## 🛠️ Développement et Débogage

### Scripts utilitaires

* **`debug.sh`** : Génère un rapport complet (`rapport_debug.txt`) sur l'état des conteneurs et les logs critiques.
    ```bash
    chmod +x debug.sh
    ./debug.sh
    ```

### Problèmes courants

* **Erreur `Bind for 0.0.0.0:8080 failed`** : Le port 8080 est déjà pris. Modifiez `AIRFLOW_WEB_PORT` dans le fichier `.env`.
* **Elasticsearch Crash (Exit 78/137)** : Manque de mémoire ou `vm.max_map_count` trop bas.
    * Linux/WSL : `sudo sysctl -w vm.max_map_count=262144`
* **Spark : `JAVA_HOME not set`** : Les images Docker fournies gèrent Java. Assurez-vous que les conteneurs `spark-master` et `spark-worker` sont bien basés sur l'image construite via `Dockerfile.spark`.