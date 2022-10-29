import pymongo
import json

#Establish connection
myclient = pymongo.MongoClient('mongodb://127.0.0.1:27017')


#Creating database
mydb = myclient['StudentsRecords']

#Creating collection
mycollection = mydb['Records']

#Reading json file
with open('students.json') as f:
    file_data = [json.loads(line) for line in f]

#Inserting json data to collection
if isinstance(file_data,list):
    mycollection.insert_many(file_data)
else:
    mycollection.insert_one(file_data)





#__________1.Find the student name who scored maximum scores in all (exam, quiz and homework)?
sum_result= mycollection.aggregate(
    [{
    "$project" :
        {'name':1,
         "Total Marks" : {"$sum" :"$scores.score"}
         }}
    ])
max = 0
for i in sum_result:
    if max<i['Total Marks']:
        max = i['Total Marks']
        name=i['name']
print(name,max)







#__________2.Find students who scored below average in the exam and pass mark is 40%?
data = mycollection.find({"scores.0.score":{"$lt":40}})
for i in data:
    print(i)







#__________3.Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.
#Exam
mycollection.update_many({"scores.0.score":{'$lt':40}},{'$set':{'scores.0.result':'fail'}})
mycollection.update_many({"scores.0.score":{'$gt':40}},{'$set':{'scores.0.result':'pass'}})
#Quiz
mycollection.update_many({"scores.1.score":{'$lt':40}},{'$set':{'scores.1.result':'fail'}})
mycollection.update_many({"scores.1.score":{'$gt':40}},{'$set':{'scores.1.result':'pass'}})
#Homework
mycollection.update_many({"scores.2.score":{'$lt':40}},{'$set':{'scores.2.result':'fail'}})
mycollection.update_many({"scores.2.score":{'$gt':40}},{'$set':{'scores.2.result':'pass'}})





#__________4.Find the total and average of the exam, quiz and homework and store them in a separate collection.
data= mycollection.find()
#initialize values as zero
sum_exam = 0
sum_quiz = 0
sum_homework = 0
data_count = 0

#calculating sum of exam,quiz,homework and total data points
for i in data:
    sum_exam = sum_exam+i['scores'][0]['score']
    sum_quiz = sum_quiz+i['scores'][1]['score']
    sum_homework = sum_homework+i['scores'][2]['score']
    data_count = data_count+1

#avarage calculation of exam,quiz,homework
avg_exam = sum_exam/data_count
avg_quiz = sum_quiz/data_count
avg_homework = sum_homework/data_count

#creating a new list to input the findings in a new collection
data =[
    {'Total_Exam':sum_exam},
    {'Avarage_Exam':avg_exam},
    {'Total_Quiz':sum_quiz},
    {'Avarage_Quiz':avg_quiz},
    {'Total_Homework':sum_homework},
    {'Avarage_Homework':avg_homework}
    ]
#Creating new collection
mycollection = mydb['Total_Avarage']

#Inserting data
mycollection.insert_many(data)





#__________5.Create a new collection which consists of students who scored below average and above 40% in all the categories.
data = mycollection.find({'$and':[{'scores.0.score':{"$gt":40}},{'scores.0.score':{"$lt":avg_exam}},
                                    {'scores.1.score':{"$gt":40}},{'scores.1.score':{"$lt":avg_quiz}},
                                    {'scores.2.score':{"$gt":40}},{'scores.2.score':{"$lt":avg_homework}}
                                 ]
                            })
#Creating new collection
mycollection = mydb['DATA_BAVG_ATOT']

#Inserting data
mycollection.insert_many(data)

#----------------------------------------------OR-------------------------------------------------------------------------------#

#collecting data where score below average and above 40% in all the categories
exam_data = mycollection.find({'$and':[{'scores.0.score':{"$gt":40}},{'scores.0.score':{"$lt":avg_exam}}]})
quiz_data = mycollection.find({'$and':[{'scores.1.score':{"$gt":40}},{'scores.1.score':{"$lt":avg_quiz}}]})
homework_data = mycollection.find({'$and':[{'scores.2.score':{"$gt":40}},{'scores.2.score':{"$lt":avg_homework}}]})

#store data to single array without repeated values
data_collect =[]
for i in exam_data:
    if i not in data_collect:
        data_collect.append(i)
for i in quiz_data:
    if i not in data_collect:
        data_collect.append(i)
for i in homework_data:
    if i not in data_collect:
        data_collect.append(i)
#Creating new collection
mycollection = mydb['DATA_BAVG_ATOT']

#Inserting data
mycollection.insert_many(data)



#__________6.Create a new collection which consists of students who scored below the fail mark in all the categories.
mycollection = mydb['Records']
data = (mycollection.find({'$and':[{'scores.0.result':'fail'},{'scores.1.result':'fail'},{'scores.2.result':'fail'}]}))
#Creating new collection
mycollection = mydb['FailData']

#Inserting data
mycollection.insert_many(data)





#__________7.Create a new collection which consists of students who scored above pass mark in all the categories.
mycollection = mydb['Records']
data = (mycollection.find({'$and':[{'scores.0.result':'pass'},{'scores.1.result':'pass'},{'scores.2.result':'pass'}]}))
#Creating new collection
mycollection = mydb['PassData']

#Inserting data
mycollection.insert_many(data)