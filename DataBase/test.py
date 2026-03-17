from db_connection import get_engine

engine = get_engine()

print(engine.connect())