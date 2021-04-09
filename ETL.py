# The Grand Tour Stages
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Sequence
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import csv

# constants
data_file = "./Data/stages_TDF.csv"

# setup database / SQL Alchemy
Base = declarative_base()
db_connection_string = "postgres:postgres@localhost:5433/TDF"

# create base classes to describe the tables and data


class TDF_Stage(Base):
    __tablename__ = 'stage'
    id = Column(Integer, primary_key=True)
    stage_sequence = Column(String)
    date = Column(DateTime, )
    distance = Column(Float)
    origin_ID = Column(Integer, ForeignKey(
        "stage_location.id"), nullable=False)
    destination_ID = Column(Integer, ForeignKey(
        "stage_location.id"), nullable=False)
    stage_Type_ID = Column(Integer, ForeignKey(
        "stage_type.id"), nullable=False)
    winner_ID = Column(Integer, ForeignKey(
        "rider.id"), nullable=False)


class Racer(Base):
    __tablename__ = 'rider'
    id = Column(Integer, primary_key=True)
    rider_name = Column(String)
    rider_country_id = Column(Integer, ForeignKey(
        "country.id"), nullable=False)


class Country(Base):
    __tablename__ = "country"
    id = Column(Integer, primary_key=True)
    country_code = Column(String)


class Stage_type(Base):
    __tablename__ = "stage_type"
    id = Column(Integer, primary_key=True)
    stage_type = Column(String)


class Stage_location(Base):
    __tablename__ = "stage_location"
    id = Column(Integer, primary_key=True)
    location_name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)

# Extract


def extract(data_file):
    data = [{}]
    # read les data
    with open(data_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))
    return data
# Transform and Load


def transform_and_load(stageData):
    # transform the data to classes
    for stage in stageData:
        try:
            exists = bool(session.query(Stage_location).filter_by(
                location_name=stage["Origin"]).first())
            if not exists:
                stage_location = Stage_location(
                    location_name=stage["Origin"])
                session.add(stage_location)
                session.commit()

            exists = bool(session.query(Stage_location).filter_by(
                location_name=stage["Destination"]).first())
            if not exists:
                stage_location = Stage_location(
                    location_name=stage["Destination"])
                session.add(stage_location)
                session.commit()

            exists = bool(session.query(Stage_type).filter_by(
                stage_type=stage["Type"]).first())
            if not exists:
                stage_type = Stage_type(
                    stage_type=stage["Type"])
                session.add(stage_type)
                session.commit()

            exists = bool(session.query(Country).filter_by(
                country_code=stage["Winner_Country"]).first())
            if not exists:
                country = Country(
                    country_code=stage["Winner_Country"])
                session.add(country)
                session.commit()

            exists = bool(session.query(Racer).filter_by(
                rider_name=stage["Winner"]).first())
            if not exists:
                rider = Racer(
                    rider_name=stage["Winner"],
                    rider_country_id=session.query(Country).filter_by(
                        country_code=stage["Winner_Country"]).value('id'))
                session.add(rider)
                session.commit()

            stage_object = TDF_Stage(
                stage_sequence=stage["Stage"],
                date=stage["Date"],
                origin_ID=session.query(Stage_location).filter_by(
                    location_name=stage["Origin"]).value('id'),
                destination_ID=session.query(Stage_location).filter_by(
                    location_name=stage["Destination"]).value('id'),
                stage_Type_ID=session.query(Stage_type).filter_by(
                    stage_type=stage["Type"]).value('id'),
                distance=stage["Distance"],
                winner_ID=session.query(Racer).filter_by(
                    rider_name=stage["Winner"]).value('id'))
            session.add(stage_object)
            session.commit()
        except KeyError:
            # YOLO
            pass
    # YOLO
    return True


# setup the DB
engine = create_engine(f'postgresql://{db_connection_string}')
# kill everything
Base.metadata.drop_all(engine)
# rebuild the tables
Base.metadata.create_all(engine)
# start the session
session = Session(bind=engine)

# buckle up and pull the throttle
transform_and_load(extract(data_file))

# clean up,
session.close_all()
