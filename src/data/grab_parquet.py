import requests
import os
from minio import Minio
 
def download_files_jan_to_aug():
    """Télécharge les fichiers de janvier à août 2024 et les enregistre localement."""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
    save_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"

    os.makedirs(save_dir, exist_ok=True)
 
    # Boucle pour télécharger chaque fichier de janvier à août
    for month in range(1, 9):  # De 1 (janvier) à 8 (août)
        month_str = f"{month:02}"  # Format de mois en deux chiffres, ex : 01, 02, ..., 08
        file_url = f"{base_url}{month_str}.parquet"
        file_name = os.path.join(save_dir, f"yellow_tripdata_2024-{month_str}.parquet")
        print(f"Téléchargement de {file_name} ...")
 
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print(f"Fichier téléchargé avec succès : {file_name}")
        else:
            print(f"Échec du téléchargement pour {file_name} avec le code : {response.status_code}")
 
def download_latest_august_file():
    """Télécharge uniquement le fichier d'août 2024 et l'enregistre localement."""
    file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-08.parquet"
    save_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"
    os.makedirs(save_dir, exist_ok=True)
    file_name = os.path.join(save_dir, "yellow_tripdata_2024-08.parquet")
   
    print(f"Téléchargement de {file_name} ...")
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(file_name, 'wb') as f:
            f.write(response.content)
        print(f"Fichier téléchargé avec succès : {file_name}")
    else:
        print(f"Échec du téléchargement pour {file_name} avec le code : {response.status_code}")
 
def upload_to_minio():
    """Transfère les fichiers Parquet téléchargés vers Minio."""
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    bucket_name = "yellow-taxi-data"
 
    # Crée le bucket s'il n'existe pas
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} créé.")
    else:
        print(f"Bucket {bucket_name} existe déjà.")
 
    # Chemin vers le dossier contenant les fichiers
    data_dir = r"C:\Users\hp\Downloads\ATL-Datamart-main\ATL-Datamart-main\data\raw"
    for file_name in os.listdir(data_dir):
        file_path = os.path.join(data_dir, file_name)
        client.fput_object(bucket_name, file_name, file_path)
        print(f"{file_name} uploadé dans le bucket {bucket_name}.")
 
if __name__ == "__main__":
    # Télécharge les fichiers de janvier à août
    download_files_jan_to_aug()
 
    # Télécharge uniquement le fichier d'août
    download_latest_august_file()
 
    # Upload des fichiers dans Minio
    upload_to_minio()