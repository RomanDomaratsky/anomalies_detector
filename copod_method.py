from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String
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
    name = Column(String)


Base.metadata.create_all(engine)

# Fetch data from your table
query = "SELECT * FROM kyiv_apartments"
df = pd.read_sql(query, engine)

# Select only numeric columns for the anomaly detection
numeric_df = df.select_dtypes(include=[float, int])

# Initialize the COPOD model
clf = COPOD()
clf.fit(numeric_df.values)

# Get the prediction labels (0 for inliers, 1 for outliers)
labels = clf.labels_

# Add the labels to original dataframe
df['anomaly'] = labels

# Identify anomalies
anomalies = df[df['anomaly'] == 1]

# Get the IDs of the anomalies
anomaly_ids = anomalies['id'].tolist()

# Perform the delete operation
stmt = delete(MyTable).where(MyTable.id.in_(anomaly_ids))
session.execute(stmt)
session.commit()

