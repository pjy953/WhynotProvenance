# query big data
import time
import psycopg2
import itertools
from collections import deque
# 12.94 second ?
# select products.title,products.price,orderlines.quantity,customers.income,customers.country from orders,customers,orderlines,products where orders.customerid = customers.customerid and orderlines.orderid = orders.orderid and products.prod_id = orderlines.prod_id and customers.income >= 100000 and customers.country = 'China' and orderlines.quantity>4 UNION select products.title,products.price,orderlines.quantity,customers.income,customers.country from orders,customers,orderlines,products where orders.customerid = customers.customerid and orderlines.orderid = orders.orderid and products.prod_id = orderlines.prod_id and orders.totalamount > 432 and price<10;
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

def querycreate(subset,place):
    naturaljoin = []
    queries = []
    text_select = " select * "
    text_from = " from "
    text_where = " where "
    for i in range(len(subset)):
        if len(subset[i]) == 3:
            if not subset[i][1] in naturaljoin:
                naturaljoin.append(subset[i][1])
            if not subset[i][2] in naturaljoin:
                naturaljoin.append(subset[i][2])
    # all match
    if len(naturaljoin) == len(place):
        for n in range(len(subset)):
            text_where = text_where + subset[n][0] + " and "
        text_where = text_where[0:len(text_where)- 4]
        for n in range(len(place)):
            text_from = text_from + place[n][0] + ","
        text_from = text_from[0:len(text_from)- 1]
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
        queries.append(text_select + text_from + text_where)
        #sub not match / not natural join
        for n in range(len(place)):
            text_from = " from "
            text_where = " where "
            if place[n][1] not in naturaljoin:
                text_from = text_from + place[n][0]
                for i in range(len(subset)):
                    if int(subset[i][1]) == n+1:
                        text_where = text_where + subset[i][0] + " and "
                text_where = text_where[0:len(text_where)- 4]
                if text_where != " wh":
                    queries.append(text_select + text_from + text_where)
                else:
                    queries.append(text_select + text_from)

    else:
        for n in range(len(place)):
            text_from = " from "
            text_where = " where "
            text_from = text_from + place[n][0]
            for i in range(len(subset)):
                if int(subset[i][1]) == n+1:
                    text_where = text_where + subset[i][0] + " and "
            text_where = text_where[0:len(text_where)- 4]
            if text_where != " wh":
                queries.append(text_select + text_from + text_where)
            else:
                queries.append(text_select + text_from)

    return queries


conn = psycopg2.connect(database="test2", user="psj953", host="127.0.0.1", port="5432")

cur = conn.cursor()
text1 = "select * "
text2 = "from "
text3 = "where "

place = []
place.append(["customer",'1'])
place.append(["orders",'2'])
place.append(["lineitem",'3'])

conditions = []
conditions.append(["o_orderdate<'1998-07-21'",'2'])
conditions.append(["l_shipdate>'1998-07-21'",'3'])
conditions.append(["c_custkey=o_custkey",'1','2'])
conditions.append(["o_orderkey=l_orderkey",'2','3'])


#select count(*) from customer as c, orders as o, lineitem as l where o_orderdate<'1998-07-21' and l_shipdate>'1998-07-21' and c_custkey=o_custkey and o_orderdate<'1996-01-01' and l_extendedprice>50000

start = time.time()
number = 0
result = []
for L in range(1, len(conditions)+1):
    for subset in itertools.combinations(conditions, L):
        text4 = "where "
        print 1
        number = number + 1
        print number
        print subset
        listanswer = []
        listanswer = listremove(subset,tuple(conditions))
        # why-not question
        # listanswer.append(["o_orderdate<'1996-01-01'",'2'])
        # listanswer.append(["l_extendedprice>50000",'3'])
        listanswer.append(["l_extendedprice>100000",'3'])
        listanswer.append(["o_orderdate=l_commitdate",'2','3'])
        listanswer.append(["c_nationkey=4",'1'])
        queries = querycreate(listanswer,place)

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

    if result:
        break;

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))
print result


conn.close()