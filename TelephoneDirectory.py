import pymongo

#Establish connection
myclient = pymongo.MongoClient('mongodb://127.0.0.1:27017')

#Creating database
mydb = myclient['TelephoneDirectory']

#Creating collection
mycollection = mydb['Directory']

data = {
    'name':'Anil Kumar',
    'phone_number': 9998880000,
    'place':'Tamilnadu'
}

#Query to insert one data point
mycollection.insert_one(data)

list_data = [
    {'name':'Arathi','phone_number':9658412320,'place':'Kerala'},
    {'name':'Babitha','phone_number':9658412320,'place':'Karnataka'},
    {'name':'Carlouse','phone_number':9658412320,'place':'Maharashtra'},
    {'name':'David','phone_number':9658412320,'place':'Goa'},
    {'name':'Emil Mathew','phone_number':9658412320,'place':'Panjab'},
    {'name':'Fahad Ali','phone_number':9658412320,'place':'Tripura'},
    {'name': 'George', 'phone_number': 9658412320,'place':'Hariyana'},
    {'name': 'Hareesh', 'phone_number': 9658412320,'place':'Sikkim'},
    {'name': 'Imran Usaf', 'phone_number': 9658412320,'place':'Gujarath'}
]

#Query to insert multiple data points
mycollection.insert_many(list_data)

#Query to fetch records created
inserted_data = mycollection.find()
for data in inserted_data:
    print(data)

#Query to modify record using update_one() method
myquery = { "place": "Tamilnadu" }
newvalues = { "$set": { "place": "Uthar Pradesh" } }
mycollection.update_one(myquery, newvalues)

#Query to delete record using delete_one() method
myquery = { "name": "Fahad Ali" }
mycollection.delete_one(myquery)

