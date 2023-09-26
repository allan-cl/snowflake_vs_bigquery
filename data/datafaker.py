import random
from datetime import datetime
import pandas as pd
from faker import Faker
from tqdm import tqdm


fake = Faker()


def generate_users_data(count):
    users_data = []
    for _ in tqdm(range(count)):
        users_data.append({
            "user_id": fake.uuid4(),
            "username": fake.name(),
            "email": fake.email()
        })
    users_df = pd.DataFrame(data=users_data, columns=['user_id', 'username', 'email'])
    users_df.to_pickle(path='faker_users.pkl')
    print(f"generated {users_df.shape[0]} user datas in faker_users.pkl")
    return users_data


def generate_orders_data(users_data):
    orders_data = []
    for user_data in tqdm(users_data):
        for _ in range(random.randint(0, 100)):
            orders_data.append({
                "order_id": fake.uuid4(),
                "user_id": user_data["user_id"],
                "product": fake.word(),
                "amount": random.randint(1, 1000),
                "order_time": datetime.strftime(fake.date_time_between(start_date="-365d", end_date="now"), "%Y-%m-%d %H:%M:%S")
            })
    orders_df = pd.DataFrame(data=orders_data, columns=['order_id', 'user_id', 'product', 'amount', 'order_time'])
    orders_df.to_pickle(path='faker_orders.pkl')
    print(f"generated {orders_df.shape[0]} order datas in faker_orders.pkl")
    return orders_data


if __name__ == "__main__":
    users_data = generate_users_data(count=10000)
    orders_data = generate_orders_data(users_data=users_data)
