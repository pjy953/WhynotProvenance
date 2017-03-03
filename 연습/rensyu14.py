# query union query
from multiprocessing import Process, Queue, Pool
import multiprocessing
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

def checktuple(query,dbname,output):
    print query
    dbname.execute(query)
    rows = dbname.fetchone()
    if rows:
        # print query,1
        output.put('yes')
        return 'yes'
    else:
        # print query,2
        output.put('no')
        return 'no'

def dummy(output):
    while True:
        output.put('0',timeout=1)

def receiver(output):
    while True:
        try:
            msg = output.get(timeout=1)
            if msg == 'yes':
                result.append(subset)
                print 333
                return 
            elif msg == 'no':
                print 555
                return
            else:
                print 666
        except output.Empty:
            print 2;
            break
def queuecheck(queue):
    try:
        msg = queue.get(timeout=60)
        if msg:
            return msg
    except queue.Empty:
        return 0

conn = psycopg2.connect(database="test2", user="psj953", host="127.0.0.1", port="5432")
conn2 = psycopg2.connect(database="test2", user="psj953", host="127.0.0.1", port="5432")

cur = conn.cursor()
cur2 = conn2.cursor()
text1 = "select * "
text2 = "from customer, orders, lineitem　"
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
        output1 = Queue(maxsize=10)
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
            query2 = text1 + text2 + text4 + " and o_orderdate<'1996-01-01' and l_extendedprice>50000 limit 1"
        else:
            query = text1 + text2 + text4 + " o_orderdate<'1996-01-01' and l_extendedprice>50000"#l_extendedprice>100000 and o_orderdate=l_commitdate and c_nationkey=4"#"
            query2 = text1 + text2 + text4 + " o_orderdate<'1996-01-01' and l_extendedprice>50000 limit 1"

        procs = []
        procs.append(Process(target=checktuple,args=(query,cur,output1)))
        procs.append(Process(target=checktuple,args=(query2,cur2,output1)))
        procs.append(Process(target=dummy,args=(output1)))
        for p in procs:
            p.start()

        # pool = multiprocessing.Pool(2)

        # procs = []
        # procs.append(pool.apply_async(checktuple,args=(query,cur,)))
        # procs.append(pool.apply_async(checktuple,args=(query2,cur2,)))

        # while True:
        # output = [p.get() for p in procs]
            
        # print procs
        # print output

        # pool.close()
        # pool.join()

        receivers=[]
        procs.append(Process(target=receiver,args=(output1)))
        for p in procs:
            p.start()

        # while True:
        #     # msg = queuecheck(output1)
        #     # if msg == 'yes':
        #     #     result.append(subset)
        #     #     print msg
        #     #     break
        #     # elif msg == 'no':
        #     #     print msg
        #     #     break
        #     try:
        #         msg = output1.get(timeout=1)
        #         if msg == 'yes':
        #             result.append(subset)
        #             print 333
        #             break
        #         elif msg == 'no':
        #             print 555
        #             break
        #     except output1.Empty:
        #         print 2;
        #         break
        print 2
        output1.close()
        print 3
        for p in procs:
            p.join()
        print 4

    if result:
        break;

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))
print result


conn.close()