import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# =========================
# CONFIGURACIÓN DEL PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",   # ← EXTERNAL listener
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


CATEGORIES = ["Tech", "Fashion", "Home", "Sports", "Beauty"]
EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase"]

CITIES = [
    {"city": "Guayaquil", "province": "Guayas", "lat": -2.1708, "lon": -79.9224},
    {"city": "Quito", "province": "Pichincha", "lat": -0.1807, "lon": -78.4678},
    {"city": "Cuenca", "province": "Azuay", "lat": -2.9001, "lon": -79.0059},
]

STORE_TYPES = ["physical_store", "online_store"]


def generate_event():
    city_info = random.choice(CITIES)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 10000),
        "category": random.choice(CATEGORIES),
        "event_type": event_type,
        "price": round(random.uniform(5, 500), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": f"ST{random.randint(1, 20):03d}",
        "store_type": random.choice(STORE_TYPES),
        "city": city_info["city"],
        "province": city_info["province"],
        "location": {
            "lat": city_info["lat"],
            "lon": city_info["lon"],
        },
    }

    return event, event_type


def get_topic_for_event(event_type: str) -> str:
    if event_type == "purchase":
        return "orders"
    elif event_type in ("add_to_cart", "remove_from_cart"):
        return "cart_events"
    else:
        return "page_views"


def main():
    print("Iniciando simulador de eventos de e-commerce. Ctrl+C para detener.\n")
    try:
        while True:
            event, event_type = generate_event()
            topic = get_topic_for_event(event_type)

            producer.send(topic, event)
            print(f"[{topic}] {event}")

            # Pequeña pausa para no saturar
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nSimulador detenido por el usuario.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
