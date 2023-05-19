import tensorflow as tf
import matplotlib.pyplot as plt
import seaborn as sns

from tensorflow import keras
from tensorflow.keras import layers


def plot_loss(history):
   plt.plot(history.history['loss'], label='loss')
   plt.plot(history.history['val_loss'], label='val_loss')
   plt.ylim([0, 10])
   plt.xlabel('Epoch')
   plt.ylabel('Error [MPG]')
   plt.legend()
   plt.grid(True)

if __name__ == "__main__":
    """
    Etape 1 : Constituer le Dataset (récupérer le dataset depuis le cluster Hadoop)
    Etape 2 : Diviser le dataset à 90 % en entrainement, 10% en validation
    Etape 3 : Construire le modèle de régression modèle
    Etape 4 : Ecrire la fonction pour lancer l'entrainement
    Etape 6 : Sauvegarder le modèle.
    """

    # train_dataset =
    # test_dataset =
    
    regression_model = tf.keras.Sequential([
        layers.Dense(units=1)
    ])

    regression_model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.1),
        loss='mean_absolute_error')


    history = regression_model.fit(
        train_features[''],
        train_labels,
        epochs=50,
        # Suppress logging.
        verbose=0,
        # Calculate validation results on 20% of the training data.
        validation_split = 0.1)
    
    regression_model.save('saved_model/my_model')

