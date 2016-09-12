# query big data
import time
import psycopg2
import itertools
from collections import deque
def tuple_without(original_tuple, element_to_remove):
    new_tuple = []
    for s in list(original_tuple):
        if not s == element_to_remove:
            new_tuple.append(s)
    return tuple(new_tuple)

def listremove(mysubset,conditions):
    new_tuple = conditions
    for i in range(len(mysubset)):
        new_tuple = tuple_without((new_tuple),mysubset[i])
    return list(new_tuple)

def querycreate(subset,place,projections):
    naturaljoin = []
    queries = []
    text_select = " select distinct "
    text_from = " from "
    text_where = " where "
    for i in range(len(subset)):
        if len(subset[i]) == 3:
            if len(naturaljoin) == 0:
                naturaljoin.append(subset[i][1])
                naturaljoin.append(subset[i][2])
            else:
                if not subset[i][1] in naturaljoin and subset[i][2] in naturaljoin:
                    naturaljoin.append(subset[i][1])
                if not subset[i][2] in naturaljoin and subset[i][1] in naturaljoin:
                    naturaljoin.append(subset[i][2])
    # all match
    if len(naturaljoin) == len(place):
        for n in range(len(subset)):
            text_where = text_where + subset[n][0] + " and "
        text_where = text_where[0:len(text_where)- 4]
        for n in range(len(place)):
            text_from = text_from + place[n][0] + ","
        text_from = text_from[0:len(text_from)- 1]
        for n in range(len(projections)):
            if projections[n][0] != "*":
                text_select = text_select + projections[n][0] + ","
        text_select = text_select[0:len(text_select)- 1]
        queries.append(text_select + text_from + text_where)

    # sub match, sub not match
    elif len(naturaljoin)>0:
        #sub match/ all natural join
        for n in range(len(subset)):
            for i in range(len(subset[n]) - 1):
                if subset[n][i+1] in naturaljoin and subset[n][0] not in text_where:
                    text_where = text_where + subset[n][0] + " and "
        text_where = text_where[0:len(text_where)- 4]
        for n in range(len(place)):
            if place[n][1] in naturaljoin and place[n][0] not in text_from:
                text_from = text_from + place[n][0] + ","
        text_from = text_from[0:len(text_from)- 1]
        for n in range(len(projections)):
            if projections[n][1] in naturaljoin:
                text_select = text_select + projections[n][0] + ","
        text_select = text_select[0:len(text_select)- 1]        
        queries.append(text_select + text_from + text_where)
        #sub not match / not natural join
        for n in range(len(place)):
            text_select =" select distinct "
            text_from = " from "
            text_where = " where "
            if place[n][1] not in naturaljoin:
                text_select = text_select + projections[place[n][1]-1][0] + ","
                text_from = text_from + place[n][0] +","
                for i in range(len(subset)):
                    if len(subset[i]) == 2:
                        if int(subset[i][1]) == n+1:
                            text_where = text_where + subset[i][0] + " and "
                    elif len(subset[i]) == 3:
                        if int(subset[i][1]) == n+1:
                            text_where = text_where + subset[i][0] + " and "
                            text_from = text_from + place[int(subset[i][2]) -1 ][0] +","
                        elif int(subset[i][2]) == n+1:
                            text_where = text_where + subset[i][0] + " and "
                            text_from = text_from + place[int(subset[i][1]) -1 ][0] +","

                text_from = text_from[0:len(text_from)- 1]
                text_where = text_where[0:len(text_where)- 4]
                text_select = text_select[0:len(text_select)-1]
                if text_where != " wh":
                    queries.append(text_select + text_from + text_where)
                else:
                    queries.append(text_select + text_from)

    else:
        for n in range(len(place)):
            text_select = "select distinct "
            text_from = " from "
            text_where = " where "
            text_from = text_from + place[n][0]
            text_select = text_select + projections[n][0]
            for i in range(len(subset)):
                if int(subset[i][1]) == n+1:
                    text_where = text_where + subset[i][0] + " and "
            text_where = text_where[0:len(text_where)- 4]
            if text_where != " wh":
                queries.append(text_select + text_from + text_where)
            else:
                queries.append(text_select + text_from)
    return queries


conn = psycopg2.connect(database="test", user="psj953", host="127.0.0.1", port="5432")

cur = conn.cursor()
text1 = "select distinct "
text2 = "from "
text3 = "where "

projections = []
projection = []
projection2 = []

places = []
place = []
place2 = []

condition = []
conditions = []
conditions2 = []
# 1, test
# place.append(["city",'1'])
# place.append(["country",'2'])
# place.append(["countrylanguage",'3'])

# conditions = []
# conditions.append(["city.countrycode = country.code",'1','2'])
# conditions.append(["countrylanguage.countrycode = country.code",'2','3'])
# conditions.append(["city.population>10000000",'1'])
# condition.append([conditions,'0'])
# conditions.append(["city.name ='Japan'",'1'])

# 2, test except
projections.append(["*",'1'])
projections.append(["country.name", '2'])

place.append(["city",'1'])
place.append(["country",'2'])
place2.append(["city",'1'])
place2.append(["country",'2'])
places.append(place)
places.append(place2)

conditions.append(["city.countrycode = country.code",'1','2'])
conditions.append(["city.population>90000000",'1'])
conditions2.append(["city.countrycode = country.code",'1','2'])
conditions2.append(["city.population<90000",'1'])
conditions2.append(["country.name = 'China'",'2'])
condition.append([conditions,'0'])
condition.append([conditions2,'1'])

# 3, test union
# place.append(["orders",'1'])
# place.append(["customers",'2'])
# place.append(["orderlines",'3'])
# place.append(["products",'4'])
# place2.append(["orders",'1'])
# place2.append(["customers",'2'])
# place2.append(["orderlines",'3'])
# place2.append(["products",'4'])
# places.append(place)
# places.append(place2)

# conditions.append(["orders.customerid = customers.customerid",'1','2'])
# conditions.append(["orderlines.orderid = orders.orderid",'3','1'])
# conditions.append(["products.prod_id = orderlines.prod_id",'4','3'])
# conditions.append(["customers.income >= 100000",'2'])
# conditions.append(["customers.country = 'China'",'2'])
# conditions.append(["orderlines.quantity>4",'3'])
# conditions2.append(["orders.customerid = customers.customerid",'1','2'])
# conditions2.append(["orderlines.orderid = orders.orderid",'1','3'])
# conditions2.append(["products.prod_id = orderlines.prod_id",'4','3'])
# conditions2.append(["orders.totalamount > 432",'1'])
# conditions2.append(["price<10",'4'])
# condition.append([conditions,'0'])
# condition.append([conditions2,'0'])



start = time.time()
number = 0
result = []
result2 = []
result_true = []

# union
# for i in range(len(condition)):
#     if i == 0:
#         for L in range(1, len(condition[i])+1):
#             for subset in itertools.combinations(condition[i], L):
#                 text4 = "where "
#                 number = number + 1
#                 listanswer = []
#                 listanswer = listremove(subset,tuple(condition[i]))
#                 # why-not question
#                 # 1, 2
#                 listanswer.append(["customers.country='China'",'2'])
#                 queries = querycreate(listanswer,places[i])
#                 for n in range(len(queries)):
#                     cur.execute(queries[n])
#                     rows = cur.fetchone()
#                     if rows:
#                         boolean_condition = True
#                     else:
#                         boolean_condition = False
#                         break

#                 if boolean_condition:
#                     result.append(subset)
#                 else:
#                     result_true.append(subset)
#             if result:
#                 break;
#     if i == 1: 
#         for L in range(1, len(condition[i])+1):
#             for subset in itertools.combinations(condition[i], L):
#                 text4 = "where "
#                 number = number + 1
#                 listanswer = []
#                 listanswer = listremove(subset,tuple(condition[i]))
#                 # why-not question
#                 # 1,2
#                 listanswer.append(["customers.country='China'",'2'])
#                 queries = querycreate(listanswer,places[i])
#                 for n in range(len(queries)):
#                     cur.execute(queries[n])
#                     rows = cur.fetchone()
#                     if rows:
#                         boolean_condition = True
#                     else:
#                         boolean_condition = False
#                         break
#                 if boolean_condition:
#                     result2.append(subset)
#                 else:
#                     result_true.append(subset)             
#             if result2:
#                 break;

# result = result + result2
# print result_true
# except
for i in range(0, len(condition)):
    if condition[i][1] == '0':
        for L in range(1, len(condition[i][0])+1):
            for subset in itertools.combinations(condition[i][0], L):
                number = number + 1
                listanswer = []
                listanswer = listremove(subset,tuple(condition[i][0]))
                # why-not question
                # 1, 2
                listanswer.append(["country.name='China'",'2'])
                # listanswer.append(["customers.country='China'",'2'])
                queries = querycreate(listanswer,places[i],projections)
                for n in range(len(queries)):
                    cur.execute(queries[n])
                    rows = cur.fetchone()
                    if rows:
                        boolean_condition = True
                    else:
                        boolean_condition = False
                        break
                if boolean_condition:
                    result.append(subset)
                else:
                    result_true.append(subset)
            if result:
                break;
    if condition[i][1] == '1': 
        for L in range(1, len(condition[i])+1):
            for subset in itertools.combinations(condition[i][0], L):
                number = number + 1
                listanswer = []
                listanswer = listremove(subset,tuple(condition[i][0]))
                # why-not question
                # 1,2
                listanswer.append(["country.name='China'",'2'])
                # listanswer.append(["customers.country='China'",'2'])
                queries = querycreate(listanswer,places[i],projections)
                print queries
                for n in range(len(queries)):
                    cur.execute(queries[n])
                    rows = cur.fetchone()
                    print rows
                    if rows:
                        boolean_condition = False
                        break
                    else:
                        boolean_condition = True

                if boolean_condition:
                    result_true.append(subset) 
                else:
                    result2.append(subset)             
            if result2:
                break;

result = result + result2
for i in range(len(result_true)):
    if result_true[i] in result:
        result.remove(result_true[i])


elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))
print result


conn.close()