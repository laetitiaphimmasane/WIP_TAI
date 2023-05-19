import pandas as pd
import numpy as np
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka grâce au fichier consummer.py.
    ---
    
    n'oubliez pas de créer de la donnée avec des valeur nulles, de fausses valeurs ( par exemple négatives pour les valeurs
    qui initialement doivent etre entre 0 et 100), et de la valeur faussement typée ( je veux par exemple une valeur
    string qui doit à la base être en int)

"""

def generate_dataFrame(col):
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos datas
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df

def add_data(df: pd.DataFrame):
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas

    # on va creer une boucle infinie, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    #  on va creer une liste avec les elements créees
    # ajouter une ligne au DataFrame
    # retourner le dataframe
    """
    i=0
    while i < 1000:
        entrance_amount = np.random.choice([46, 'forty-five', 1, 7, 49, 2, 42, 20, 16,10, 32, 23, 9, 50])
        exit_amount = np.random.choice(['seven', None, 12, 457, 20, 0, 190, 46, 12,22,14, 5])
        temperature = np.random.choice([0.0, -1.2, 23, 17, 18.6, 20.3, None, 9, -30, 2.56 ])
        humidity = np.random.normal(loc=50, scale=10)
        parking_entrance = np.random.randint(0, 180)
        parking_exit= np.random.choice([8, None, -1, 12, 457, 20, 0, 190, 46, 12,22,14, 5])
        parking_actual_vehicle = np.random.randint(low=0, high=500)

        df.loc[i] = [entrance_amount, exit_amount, temperature, humidity, parking_entrance, parking_exit, parking_actual_vehicle]
        i += 1

    return df


def write_data_minio(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Minio.
    (Obligatoire)
    ## on va tester si la bucket existe , dans le cas contraire on la créer
    ## on pousse le dataframe sur minio
    #decommentez le code du dessous

    """

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_csv("donnes_capteurs_" + str(timestamp) + ".csv", encoding='utf-8', index=False)
    client.fput_object("donnes-capteurs", "donnes_capteurs_" + str(timestamp) + ".csv",  "donnes_capteurs_" + str(timestamp) + ".csv")


if __name__ == "__main__":

    """""
    créer une liste columns qui contient le header de votre dataframe 
    decommentez le code du dessous    
    """""
    columns = ["entrance_amount", "exit_amount", "temperature", "humidity", "parking_entrance", "parking_exit", "parking_actual_vehicle"]
    df = generate_dataFrame(columns)
    write_data_minio(df)