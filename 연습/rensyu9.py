# query union query
import time
import psycopg2
import itertools
from collections import deque

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
conn = psycopg2.connect(database="test", user="pjy953", host="127.0.0.1", port="5432")

cur = conn.cursor()
text1 = "select products.title,products.price,orderlines.quantity,customers.income,customers.country from orders,customers,orderlines,products "
text2 = "where "
conditions = []
conditions.append("orders.customerid = customers.customerid")
conditions.append("orderlines.orderid = orders.orderid")
conditions.append("products.prod_id = orderlines.prod_id")
conditions.append("customers.income >= 100000")
conditions.append("customers.country = 'China'")
conditions.append("orderlines.quantity>4")
conditions2 = []

conditions2.append("orders.customerid = customers.customerid")
conditions2.append("orderlines.orderid = orders.orderid")
conditions2.append("products.prod_id = orderlines.prod_id")
conditions2.append("orders.totalamount > 432")
conditions2.append("price<10")

text2_example2 = "where "
text3_example2 = "where "
for n in range(len(conditions)):
    text2_example2 = text2_example2 + conditions[n] + " and "
text2_example2 = text2_example2[0:len(text2_example2)- 4]
for n in range(len(conditions2)):
    text3_example2 = text3_example2 + conditions2[n] + " and "
text3_example2 = text3_example2[0:len(text3_example2)- 4]

text_example2 = text1 + text2_example2 + " union " + text1 + text3_example2

print text_example2
cur.execute(text_example2)
rows = cur.fetchall()
if rows:
    print "answer"
else:
    print "no answer"

result = []
result_true = []

subquery = []
subquery.append(text1 + text2_example2 +" and customers.country='China'")
subquery.append(text1 + text3_example2 +" and customers.country='China'")

start = time.time()

for i in range(len(subquery)):
    cur.execute(subquery[i])
    rows = cur.fetchall()
    if i == 1:
        if not rows:
            for L in range(1, max(len(conditions),len(conditions2))+1):
                for subset in itertools.combinations(conditions2, L):
                    text4 = "where "
                    listanswer = []
                    listanswer = listremove(subset,tuple(conditions2))
                    for n in range(len(listanswer)):
                        text4 = text4 + listanswer[n] + " and "
                    if not text4 == "where ":
                        text4 = text4[0:len(text4)- 4]
                        query = text1 + text4 + " and customers.country='China'"
                    else:
                        query = text1 + text4 + " customers.country='China'"
                    cur.execute(query)
                    rows = cur.fetchall()
                    if rows:
                        result.append(subset)
                    else:
                        result_true.append(subset)
                if result:
                    break
        else:
            for subset in itertools.combinations(conditions2,1):
                result_true.append(subset)
    elif i == 0:
        if not rows:
            for L in range(1, max(len(conditions),len(conditions2))+1):
                for subset in itertools.combinations(conditions, L):
                    text4 = "where "
                    listanswer = []
                    listanswer = listremove(subset,tuple(conditions))
                    for n in range(len(listanswer)):
                        text4 = text4 + listanswer[n] + " and "
                    if not text4 == "where ":
                        text4 = text4[0:len(text4)- 4]
                        query = text1 + text4 + " and customers.country='China'"
                    else:
                        query = text1 + text4 + " customers.country='China'"
                    cur.execute(query)
                    rows = cur.fetchall()
                    if rows:
                        result.append(subset)
                    else:
                        result_true.append(subset)
                if result:
                    break
        else:
            for subset in itertools.combinations(conditions,1):
                result_true.append(subset)
for i in range(len(result_true)):
    if result_true[i] in result:
        result.remove(result_true[i])
elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))
print result


conn.close()