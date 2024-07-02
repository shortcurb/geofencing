from database import Database
import time

db = Database()
db.connect('functiondb')
a = db.execute_query('SELECT * FROM functionstate')
print(a)