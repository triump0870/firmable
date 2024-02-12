from tables import Base, engine

# create all tables
Base.metadata.create_all(engine)
