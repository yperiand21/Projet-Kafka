from kafka import KafkaProducer
import requests
import json
import time

# Configuration du producteur
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialise les données en JSON
)

def send_data(topic, key, value):
    # Envoyer le message au topic Kafka
    producer.send(topic, key=key.encode('utf-8'), value=value)
    producer.flush()

def main():
    try:
        # URL de l'API des objets trouvés dans les gares SNCF
        api_url = 'https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/objets-trouves-gares/records?limit=20'
        
        # Effectuer la requête GET à l'API
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()

            # Parcourir les données reçues et les envoyer aux topics Kafka
            for record in data.get('records', []):
                fields = record.get('fields', {})

                # Exemple d'envoi des données
                send_data('type_objet_perdu', fields.get('type', 'Inconnu'), fields)
                send_data('gare_objet_perdu', fields.get('gare', 'Inconnue'), fields)

                time.sleep(1)  # Pause pour simuler l'envoi de données en temps réel
        else:
            print(f"Erreur lors de la requête à l'API. Code de statut : {response.status_code}")

    except KeyboardInterrupt:
        print("Arrêt du producteur")
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
