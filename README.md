# Big Data Project

Ce projet déploie une architecture Big Data complète et conteneurisée en local. Il intègre un Data Lake (MinIO), un cluster de traitement distribué (Spark), un orchestrateur de workflows (Airflow) et une stack d'indexation et de visualisation (Elasticsearch & Kibana).

## Prérequis

Ce projet est conçu pour fonctionner sur n'importe quel système d'exploitation capable d'exécuter Docker :

* **Linux** (Ubuntu, Debian, Fedora, etc.)
* **macOS** (Intel ou Apple Silicon)
* **Windows** (via WSL2 recommandé)

**Outils nécessaires :**
* [Docker](https://docs.docker.com/get-docker/) et [Docker Compose](https://docs.docker.com/compose/install/) (version V2 recommandée).
* Git.
* Un terminal Bash ou compatible (pour les scripts de maintenance).

## Installation

1.  **Cloner le projet** :
    ```bash
    git clone https://github.com/tortafrate/big-data-project.git
    cd big-data-project-agdb__archi
    ```

2.  **Configuration de l'environnement** :
    Le projet utilise des variables d'environnement pour définir les versions et les identifiants. Créez votre fichier `.env` à partir de l'exemple fourni :
    ```bash
    cp .env.exemple .env
    ```
    *Note : Vous pouvez éditer le fichier `.env` pour modifier les ports ou les mots de passe si nécessaire.*

3.  **Dépendances Python** :
    Les librairies Python nécessaires (comme `pyspark`, `pandas`, `apache-airflow-providers-...`) sont listées dans le fichier `requirements.txt`.
    Elles sont **automatiquement installées** à l'intérieur des conteneurs Airflow lors du démarrage. Vous n'avez aucune installation manuelle à faire sur votre machine hôte.

## Exécution du projet

Démarrez l'ensemble de l'infrastructure en arrière-plan :

```bash
docker compose up -d

```

### Accès aux services

Une fois les conteneurs (Airflow, Spark, MinIO, ELK) démarrés, accédez aux interfaces via votre navigateur :

* **Airflow Webserver** : [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
* *Credentials* : définis dans `.env` (défaut : `airflow` / `airflow`).


* **Spark Master** : [http://localhost:9090](https://www.google.com/search?q=http://localhost:9090)
* *Port configuré via SPARK_WEB_PORT dans le .env*.


* **MinIO Console** : [http://localhost:9001](https://www.google.com/search?q=http://localhost:9001)
* *Credentials* : définis dans `.env` (défaut : `minioadmin` / `minioadmin`).


* **Kibana** : [http://localhost:5601](https://www.google.com/search?q=http://localhost:5601)

Les ports peuvent être modifiés directement dans le fichier `.env`.

## Débogage

Un script `debug.sh` est fourni pour générer un rapport sur l'état des conteneurs et des logs.

```bash
chmod +x debug.sh
./debug.sh

```

Le rapport sera sauvegardé dans `rapport_debug.txt`.

**Compatibilité du script** : Ce script a été validé sous **Fedora 43**. Étant écrit en Bash standard utilisant les commandes Docker CLI, il est également utilisable sur la plupart des distributions Linux, macOS, et Windows (via Git Bash ou WSL).

## Gestion des erreurs courantes

### 1. Port déjà utilisé (Port already allocated)

**Erreur :** `Bind for 0.0.0.0:8080 failed: port is already allocated`
**Cause :** Un autre service sur votre machine utilise déjà ce port.
**Solution :**

* Arrêtez le service conflictuel.
* Ou modifiez le port dans le fichier `.env` (par exemple, changez `AIRFLOW_WEB_PORT` à `8081`) et relancez `docker compose up -d`.

### 2. Nom de conteneur déjà emprunté

**Erreur :** `The container name "/airflow_webserver" is already in use by container "..."`
**Cause :** Un ancien conteneur mal arrêté porte le même nom.
**Solution :**
Supprimez les conteneurs existants du projet :

```bash
docker compose down
# Si l'erreur persiste sur un conteneur spécifique :
docker rm -f airflow_webserver

```

### 3. Elasticsearch s'arrête immédiatement (Exit Code 78 ou 137)

**Cause :** Souvent dû à une mémoire insuffisante ou une configuration système `vm.max_map_count` trop basse sur Linux.
**Solution :**
Augmentez la limite système (sur Linux/WSL) :

```bash
sudo sysctl -w vm.max_map_count=262144

```