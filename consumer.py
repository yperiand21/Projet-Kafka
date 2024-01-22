from kafka import KafkaConsumer
import json

# Configuration du consommateur pour les deux topics
consumer = KafkaConsumer(
    'type_objet_perdu', 'gare_objet_perdu',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Commence à lire depuis le début si aucun offset n'est enregistré
    group_id='groupe_consommateur_sncf',  # Identifie le groupe de consommateurs
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialise les données JSON reçues
)

def process_message(message):
    if message.topic == 'type_objet_perdu':
        print(f"Type d'objet perdu: {message.key}, Données: {message.value}")
    elif message.topic == 'gare_objet_perdu':
        print(f"Gare où l'objet a été perdu: {message.key}, Données: {message.value}")
    else:
        print(f"Topic inconnu: {message.topic}")

def main():
    try:
        for message in consumer:
            process_message(message)
            consumer.commit()  # Commit explicitement l'offset après l'affichage
    except KeyboardInterrupt:
        print("Arrêt du consommateur")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
