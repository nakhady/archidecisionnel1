import gc
import sys
from io import BytesIO
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine
 
def ecrire_donnees_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Insère un DataFrame dans le moteur de base de données.
    Paramètres :
        - dataframe (pd.DataFrame) : Le DataFrame à insérer dans la base de données.
    Retourne :
        - bool : True si la connexion à la base de données et l'insertion sont réussies, False en cas d'échec.
    """
    config_db = {
        "moteur_db": "postgresql",
        "utilisateur_db": "postgres",
        "mot_de_passe_db": "admin",
        "ip_db": "localhost",
        "port_db": "15432",
        "nom_base_db": "nyc_warehouse",
        "table_db": "nyc_raw"
    }
    config_db["url_base_de_donnees"] = (
        f"{config_db['moteur_db']}://{config_db['utilisateur_db']}:{config_db['mot_de_passe_db']}@"
        f"{config_db['ip_db']}:{config_db['port_db']}/{config_db['nom_base_db']}"
    )
    try:
        moteur = create_engine(config_db["url_base_de_donnees"], echo=True)
        with moteur.connect() as connection:
            with connection.begin():
                print("Connexion réussie ! Traitement du fichier Parquet")
                print(f"Nombre de lignes dans le DataFrame : {len(dataframe)}")
                dataframe.to_sql(config_db["table_db"], con=connection, index=False, if_exists='append', method='multi')
                print("Insertion réussie.")
        return True
    except Exception as e:
        print(f"Erreur lors de l'insertion dans la base de données : {e}")
        return False
 
def nettoyer_nom_colonnes(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Prend un DataFrame et met les noms de colonnes en minuscules.
    Paramètres :
        - dataframe (pd.DataFrame) : Le DataFrame dont les colonnes doivent être modifiées.
    Retourne :
        - pd.DataFrame : Le DataFrame modifié avec des noms de colonnes en minuscules.
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe
 
def telecharger_et_traiter_fichier_specifique_depuis_minio(nom_bucket: str, fichier_cible: str):
    """
    Télécharge un fichier spécifique Parquet depuis Minio et l'insère directement dans la base de données.
    Paramètres :
        - nom_bucket (str) : Le nom du bucket Minio.
        - fichier_cible (str) : Le nom exact du fichier Parquet à traiter.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    try:
        # Vérification de la présence du fichier dans le bucket
        objets = client.list_objects(nom_bucket)
        fichiers_disponibles = [obj.object_name for obj in objets]
        if fichier_cible not in fichiers_disponibles:
            print(f"Le fichier {fichier_cible} n'a pas été trouvé dans le bucket {nom_bucket}.")
            return
        # Récupérer le fichier Parquet en tant que flux binaire
        file_stream = client.get_object(nom_bucket, fichier_cible)
        # Charger le flux binaire dans un tampon en mémoire
        file_data = BytesIO(file_stream.read())
        # Lire le fichier Parquet depuis le tampon
        dataframe_parquet: pd.DataFrame = pd.read_parquet(file_data, engine='pyarrow')
        # Nettoyer les noms des colonnes et insérer dans la base de données
        nettoyer_nom_colonnes(dataframe_parquet)
        if ecrire_donnees_postgres(dataframe_parquet):
            print(f"Fichier {fichier_cible} traité et inséré avec succès.")
        else:
            print(f"Échec de l'insertion pour le fichier {fichier_cible}.")
        del dataframe_parquet
        gc.collect()
    except Exception as e:
        print(f"Erreur lors du traitement du fichier {fichier_cible}: {e}")
 
def main() -> None:
    # Spécifiez ici le fichier à traiter
    fichier_cible = "yellow_tripdata_2024-01.parquet"
    telecharger_et_traiter_fichier_specifique_depuis_minio("yellow-taxi-data", fichier_cible)
 
if __name__ == '__main__':
    sys.exit(main())
