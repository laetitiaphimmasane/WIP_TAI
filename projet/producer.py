from kafka import KafkaProducer
import datetime
import numpy as np


def add_datatokafka():
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas

    Prenez la base de la fonction addData() dans le fichier capteur.py
    """

    # on va creer une boucle infinie, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    #  on va creer une liste avec les elements créees
    # on va crée le produceur kafka qui envoie la liste sur le topic qu'on a crée sur conduktor
    # temps_execution =
    current_timestamp = datetime.datetime.now()
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    while 1:
        ## Dans cette boucle, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
        ## Chaque itération comportera 1 ligne à ajouter.
        new_timestamp = current_timestamp + datetime.timedelta(seconds=1)
        entrance_amount = np.random.randint(low=0, high=50)
        exit_amount = np.random.randint(low=0, high=50)
        temperature = np.random.normal(loc=20, scale=5)
        humidity = np.random.normal(loc=50, scale=10)
        parking_entrance = np.random.randint(low=1, high=5)
        parking_exit = np.random.randint(low=1, high=5)
        parking_actual_vehicle = np.random.randint(low=0, high=500)

        row = [new_timestamp.isoformat(), entrance_amount, exit_amount, temperature, humidity, parking_entrance, parking_exit,
               parking_actual_vehicle]

        # ajouter une ligne au DataFrame
        producer.send("my-topic", row)



def write_data_kafka(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Kafka.
    (Optionnel)
    """
    """csv_string = df.to_csv(index=False)
    producer = KafkaProducer( bootstrap_servers=['127.0.0.1:9092'])
    producer.send('nom_du_topic', value=bytes(csv_string, 'utf-8'))
    producer.close()
    """

if __name__ == "__main__":
    add_datatokafka()