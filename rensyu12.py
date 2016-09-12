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
	# for i in range(0, len(mysubset)):
	# 	conditions.pop(mysubset[i])
conn = psycopg2.connect(database="test2", user="psj953", host="127.0.0.1", port="5432")

cur = conn.cursor()
text1 = "select * "
text2 = "from customer as c, orders as o, lineitem as l "
text3 = "where "

conditions = []
conditions.append("o_orderdate<'1998-07-21'")
conditions.append("l_shipdate>'1998-07-21'")
conditions.append("c_custkey=o_custkey")
conditions.append("o_orderkey=l_orderkey")
    
#select * from customer as c, orders as o, lineitem as l where o_orderdate<'1998-07-21' and l_shipdate>'1998-07-21' and c_custkey=o_custkey and o_orderdate<'1996-01-01' and l_extendedprice>50000

start = time.time()
number = 0
result = []
for L in range(1, len(conditions)+1):
    for subset in itertools.combinations(conditions, L):
        print 1
        number = number + 1
        print number
        print subset
        text4 = "where "
        listanswer = []
        listanswer = listremove(subset,tuple(conditions))
        for n in range(len(listanswer)):
            text4 = text4 + listanswer[n] + " and "
        if not text4 == "where ":
            text4 = text4[0:len(text4)- 4]
            query = text1 + text2 + text4 + " and o_orderdate<'1996-01-01' and l_extendedprice>50000"#l_extendedprice>100000 and o_orderdate=l_commitdate and c_nationkey=4"#
        else:
            query = text1 + text2 + text4 + " o_orderdate<'1996-01-01' and l_extendedprice>50000"#l_extendedprice>100000 and o_orderdate=l_commitdate and c_nationkey=4"#"
        cur.execute(query)
        elapsed_time = time.time() - start
        print("elapsed_time:{0}".format(elapsed_time))
        rows = cur.fetchone()
        print rows
        elapsed_time = time.time() - start
        print("elapsed_time:{0}".format(elapsed_time))
        if rows:
            result.append(subset)
        elapsed_time = time.time() - start
        print("elapsed_time:{0}".format(elapsed_time))
        # if result:
        #     if number == 2:
        #         break;

    if result:
        break;

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))
print result


conn.close()