from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# dla każdego usera przechowuję listę czasów transakcji
user_history = defaultdict(list)

print("Wykrywanie anomalii: więcej niż 3 transakcje w ciągu 60 sekund...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    tx_time = datetime.fromisoformat(tx['timestamp'])
    amount = tx['amount']

    # dodanie czasu i kwoty
    user_history[user_id].append((tx_time, amount))

    # zostawienie tylko transakcje z ostatnich 60 sekund
    threshold = tx_time - timedelta(seconds=60)
    user_history[user_id] = [
        (t, a) for (t, a) in user_history[user_id] if t >= threshold
    ]

   # jeśli użytkownik ma więcej niż 3 transakcje w ciągu 60 sekund -> alert
    if len(user_history[user_id]) > 3:
        total = sum(a for (t, a) in user_history[user_id])

        print(
            f"ALERT: {user_id} | {len(user_history[user_id])} transakcje w ciągu 60s | "
            f"suma: {total:.2f} PLN"
        )
        
for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    tx_time = datetime.fromisoformat(tx['timestamp'])

    # dodawanie aktualnej transakcji do historii użytkownika
    user_history[user_id].append(tx_time)

    # zostawienie tylko transakcje z ostatnich 60 sekund
    threshold = tx_time - timedelta(seconds=60)
    user_history[user_id] = [t for t in user_history[user_id] if t >= threshold]

    # jeśli użytkownik ma więcej niż 3 transakcje w ciągu 60 sekund -> alert
    if len(user_history[user_id]) > 3:
        print(
            f"ALERT: {user_id} | {len(user_history[user_id])} transakcje w ciągu 60s | "
            f"{tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}"
        )
