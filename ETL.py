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

# Extract - generic function to read the file and return a list of dictionaries


def extract(data_file):
    data = [{}]
    # read les data
    with open(data_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))
    return data
# Transform and Load - takes the data as a list of dictionaries and loads into the DB in 3NF


def transform_and_load(stageData):
    # transform the data to classes
    for stage in stageData:
        try:
            # yes this check for existence should be a function but whatever.  Query the DB and convert to a bool value, insert if it doesn't exist
            exists = bool(session.query(Stage_location).filter_by(
                location_name=stage["Origin"]).first())
            # if it doesn't exist load it
            if not exists:
                stage_location = Stage_location(
                    location_name=stage["Origin"])
                session.add(stage_location)
                session.commit()  # if we don't commit the later stages won't find it

            # see above
            exists = bool(session.query(Stage_location).filter_by(
                location_name=stage["Destination"]).first())
            if not exists:
                stage_location = Stage_location(
                    location_name=stage["Destination"])
                session.add(stage_location)
                session.commit()

            # see above
            exists = bool(session.query(Stage_type).filter_by(
                stage_type=stage["Type"]).first())
            if not exists:
                stage_type = Stage_type(
                    stage_type=stage["Type"])
                session.add(stage_type)
                session.commit()

            # see above
            exists = bool(session.query(Country).filter_by(
                country_code=stage["Winner_Country"]).first())
            if not exists:
                country = Country(
                    country_code=stage["Winner_Country"])
                session.add(country)
                session.commit()

            # see above, but special first special case,  there is a on the country column!
            exists = bool(session.query(Racer).filter_by(
                rider_name=stage["Winner"]).first())
            if not exists:
                # notice the inline query to search for the value of the FK relationship in another table
                rider = Racer(
                    rider_name=stage["Winner"],
                    rider_country_id=session.query(Country).filter_by(
                        country_code=stage["Winner_Country"]).value('id'))
                session.add(rider)
                session.commit()

            # now that all of the dependant ( child ) data is loaded, we can load the top level ( parent ) data
            # all of the inline queries!
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
            # YOLO - Not really needed BUT there was an extra line at the end of the file and I didn't want to delete it
            pass
    # YOLO - returning True for no reason at all .  just here in case I decide to check for this function executing properly
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

# clean up, like a boy scout yo
session.close_all()
