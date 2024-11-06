from minio import Minio
import requests
import os
from bs4 import BeautifulSoup
import sys

def main():
    print("Démarrage du téléchargement des données de janvier à août.")
    grab_data_jan_to_aug()
    
    print("Démarrage du téléchargement des données les plus récentes.")
    grab_latest_data()
    
    print("Transfert des données dans MinIO.")
    write_data_minio()

def grab_data_jan_to_aug():
    """Télécharge les datasets de janvier 2024 à août 2024 et les enregistre localement."""
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Section pour les liens 2024
    link_section = soup.select('#faq2024 a')  # Mis à jour pour la section 2024
    target_months = [f"2024-0{i}" for i in range(1, 9)]  # janvier à août 2024
    
    # Chemin de sauvegarde
    save_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"
    os.makedirs(save_dir, exist_ok=True)
    
    for link in link_section:
        file_url = link['href']
        if any(month in file_url for month in target_months) and "yellow" in file_url.lower():
            if not file_url.startswith("http"):
                file_url = "https://www.nyc.gov" + file_url
            download_file(file_url, save_dir)

def grab_latest_data():
    """Télécharge le dataset le plus récent disponible et l'enregistre localement."""
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Récupérer le lien le plus récent qui contient "yellow"
    link_section = soup.select('#faq2024 a')  # Mis à jour pour la section 2024
    for link in link_section:
    
        latest_link = link['href']
    
        if "yellow" in latest_link.lower():
            if not latest_link.startswith("http"):
                latest_link = "https://www.nyc.gov" + latest_link
            break  # Sort de la boucle après avoir trouvé le premier lien "yellow"
    
    save_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"
    os.makedirs(save_dir, exist_ok=True)
    download_file(latest_link, save_dir)

def download_file(file_url, save_dir):
    """Télécharge un fichier depuis un URL et le sauvegarde dans un répertoire."""
    file_name = os.path.join(save_dir, file_url.split("/")[-1])
    print(f"Téléchargement de {file_name}...")
    file_data = requests.get(file_url)
    with open(file_name, 'wb') as f:
        f.write(file_data.content)

def write_data_minio():
    """
    Transfère les fichiers Parquet téléchargés vers Minio.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket_name = "yellow-taxi-data-test"
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} créé.")
    else:
        print(f"Bucket {bucket_name} existe déjà.")

    
    
    data_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"
    if not os.listdir(data_dir):
        print("Aucun fichier trouvé dans le répertoire des données.")
    else:
        for file_name in os.listdir(data_dir):
         file_path = os.path.join(data_dir, file_name)
        client.fput_object(bucket_name, file_name, file_path)
        print(f"{file_name} uploadé dans le bucket {bucket_name}.")


if __name__ == '__main__':
    sys.exit(main())



