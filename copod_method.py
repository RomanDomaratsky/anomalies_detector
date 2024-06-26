from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float
from pyod.models.copod import COPOD
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class MyTable(Base):
    __tablename__ = 'kyiv_apartments'
    id = Column(Integer, primary_key=True)
    flat_id = Column(Integer)
    name = Column(String)
    price = Column(Integer)
    area_total = Column(Float)
    room_count = Column(Integer)


Base.metadata.create_all(engine)

# Fetch data from your table
query = "SELECT * FROM kyiv_apartments"
df = pd.read_sql(query, engine)
train_df = pd.read_csv('kyiv_apartments.csv')
train_df = train_df[['price']]

numeric_df = df[['price']]

# Initialize the COPOD model
clf = COPOD(contamination=0.01)
clf.fit(train_df.values)

# Get the prediction labels (0 for inliers, 1 for outliers)

labels = clf.predict(numeric_df.values)

# Add the labels to the original dataframe
df['anomaly'] = labels

# Identify anomalies
anomalies = df[df['anomaly'] == 1]

# Get the IDs of the anomalies
anomaly_ids = anomalies['id'].tolist()
print("Number of anomalies to delete: ", len(anomaly_ids))
print("Anomalies id: ", anomaly_ids)

# Identify duplicate entries based on 'id'
duplicate_ids = df[df.duplicated(subset=['flat_id'])]
duplicate_ids = duplicate_ids['id'].tolist()
print("Number of duplicates to delete: ", len(duplicate_ids))
print("Duplicates id: ", duplicate_ids)

# Combine anomaly and duplicate IDs
all_ids_to_delete = list(set(duplicate_ids + anomaly_ids))

if all_ids_to_delete:
    # Perform the delete operation
    stmt = delete(MyTable).where(MyTable.id.in_(all_ids_to_delete))
    session.execute(stmt)
    session.commit()

session.close()
