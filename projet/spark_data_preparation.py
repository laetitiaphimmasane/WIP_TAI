"""""
    dans ce fichier on va faire de la datapreparations
    en spark on va lire le fichier dans la bucket minio
    enregistrer la données en dataframe
    et reecrire dans une nouvelles bucket minIO
    
"""""

from minio import Minio
from minio.error import S3Error
import pandas as pd
import json
import datetime
import os

def collect_data():

    client = Minio( #connexion Minio
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    found = client.bucket_exists("donnes-capteurs")
    if not found:
        client.make_bucket("donnes-capteurs")
    else:
        print("Bucket 'donnes-capteurs' existe déjà")

    timestamp = datetime.datetime.now().strftime('%d-%l-%y')
    response = []

    buckets = client.list_buckets()
    for bucket in buckets:
        if(bucket.name == 'donnes-capteurs'): #téléchargement des fichiers data
            for item in client.list_objects('donnes-capteurs',recursive=True):
                client.fget_object(bucket.name,item.object_name,item.object_name)

def convert_dataframe():
    for filename in os.listdir("./"):
        f = os.path.join("./", filename)
        # checking if it is a file
        if (os.path.isfile(f) and ".csv" in f):
            df = pd.read_csv(f)
            new_df = clean_data(df)
            sending_df_to_minio(new_df)


def clean_data(df):
    #Remplacer les string par la moyenne de la colonne
    df = df.replace('forty-five', 0)
    df = df.replace('seven', 0)

    df = df.astype({'entrance_amount':'float','exit_amount':'float'})
    mean_temp = df["temperature"].dropna().mean()
    mean_entrance = df["entrance_amount"].dropna().mean()
    mean_exit = df["exit_amount"].dropna().mean()
    mean_park = df["parking_exit"].dropna().mean()

    #Remplacer les valeurs vides par la moyenne de la colonne
    df["temperature"] = df["temperature"].fillna(mean_temp, inplace=False)
    df["entrance_amount"] = df["entrance_amount"].fillna(df["entrance_amount"].dropna().mean(), inplace=False)
    df["exit_amount"] = df["exit_amount"].fillna(df["exit_amount"].mean(), inplace=False)
    df["parking_exit"] = df["parking_exit"].fillna(df["parking_exit"].mean(), inplace=False)

    #Remplacer les valeurs négatives par la moyenne de la colonne
    mean_park_ex = df[df["parking_exit"] > 0].mean()
    df[df["parking_exit"] < 0] = mean_park_ex

    return df


def sending_df_to_minio(df):

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    found = client.bucket_exists("clean-donnes-capteurs")
    if not found:
        client.make_bucket("clean-donnes-capteurs")

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_csv("clean_donnes_capteurs_" + str(timestamp) + ".csv", encoding='utf-8', index=False)
    client.fput_object("clean-donnes-capteurs", "clean_donnes_capteurs_" + str(timestamp) + ".csv",  "clean_donnes_capteurs_" + str(timestamp) + ".csv")


if __name__ == "__main__":
    collect_data()
    convert_dataframe()