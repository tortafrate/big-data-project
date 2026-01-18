import requests
from bs4 import BeautifulSoup
import logging
import boto3  # Remplacement de s3fs
from botocore.client import Config
import json
import os
from datetime import datetime

class Scraper:
    def __init__(self):
        # Configuration MinIO depuis les variables d'environnement
        self.minio_endpoint = "http://minio:9000"
        self.minio_user = os.getenv('MINIO_ROOT_USER', 'minioadmin')
        self.minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
        self.bucket_name = "datalake"
        
        # Initialisation du client S3 (boto3)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.minio_user,
            aws_secret_access_key=self.minio_password,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1' # Requis par boto3 même pour MinIO
        )

    def get_urls_from_sitemap(self, sitemap_url):
        """Récupère toutes les URLs d'annonces présentes dans le sitemap XML en ligne."""
        logging.info(f"Récupération du sitemap : {sitemap_url}")
        try:
            response = requests.get(sitemap_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            urls = [loc.text for loc in soup.find_all('loc')]
            
            logging.info(f"{len(urls)} URLs trouvées dans le sitemap.")
            return urls
        except Exception as e:
            logging.error(f"Erreur lors de la lecture du sitemap : {e}")
            return []

    def scrape_ad(self, url):
        """Scrape une page d'annonce spécifique et retourne un dictionnaire brut."""
        try:
            logging.info(f"Scraping : {url}")
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                logging.warning(f"Statut {response.status_code} pour {url}")
                return None

            # On garde le contenu brut (Layer RAW) + métadonnées
            page_soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraction basique pour validation
            title = page_soup.title.get_text(strip=True) if page_soup.title else "No Title"
            
            return {
                "url": url,
                "title": title,
                "html_content": str(page_soup), 
                "scraped_at": datetime.now().isoformat(),
                "source": "airsoft-occasion"
            }
        except Exception as e:
            logging.error(f"Erreur scraping {url}: {e}")
            return None

    def save_to_datalake(self, data, execution_date):
        """Sauvegarde les données dans MinIO avec un chemin partitionné par date."""
        if not data:
            logging.warning("Aucune donnée à sauvegarder.")
            return

        # Structure Clean Naming : raw/source/entity/year/month/day
        year = execution_date.strftime('%Y')
        month = execution_date.strftime('%m')
        day = execution_date.strftime('%d')
        timestamp = execution_date.strftime('%H%M%S')
        
        # Nom de fichier unique pour ce lot
        filename = f"ads_batch_{timestamp}.json"
        # Note: Boto3 n'utilise pas le nom du bucket dans la clé (key)
        key_path = f"raw/airsoft/ads/year={year}/month={month}/day={day}/{filename}"
        
        logging.info(f"Sauvegarde vers s3://{self.bucket_name}/{key_path}")
        
        try:
            # Conversion en JSON String
            json_data = json.dumps(data, ensure_ascii=False)
            
            # Écriture via Boto3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key_path,
                Body=json_data,
                ContentType='application/json'
            )
            logging.info("Sauvegarde réussie.")
        except Exception as e:
            logging.error(f"Erreur d'écriture MinIO : {e}")
            raise e