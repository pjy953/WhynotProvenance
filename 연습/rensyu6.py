import psycopg2
import itertools
import time
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
	# for i in range(0, len(mysubset)):
	# 	conditions.pop(mysubset[i])

def traverse(node):
    text = ""
    stack = deque([node])
    while stack:
        node = stack.popleft()
        text = text + str(node.data)
        if node.data == " where ":
            for n in range(len(node.children)):
                text = text + node.children[n].data + " and "
            text = text[2:len(text)- 4]
            return text
        stack.extendleft(reversed(node.children))

def search(node):
    nodes = []
    stack = deque([node])
    while stack:
        node = stack.popleft()
        if node.data == 1 or node.data == -1:
            query = queries(node)
            query.add_condition(node.data)
            # print type(node.data)
            nodes.append(query)
            # nodes.append(node)
        stack.extendleft(reversed(node.children))

    return nodes

class Node(object):
    def __init__(self, data):
        self.data = data
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

class queries(object):
    def __init__(self,data):
        self.data = data
        self.condition = 1
    def add_condition(self, obj):
        self.condition = 1 * obj

        

# select distinct country.name from city,country where city.countrycode = country.code and city.population>9000000 except select distinct country.name from city,country where city.countrycode = country.code and city.population<90000;
# select country.name,country.region,language,percentage from country,countrylanguage where country.code = countrycode and language='English' and percentage>80.0 except (select country.name,country.region,language,percentage from country,countrylanguage where countrycode=country.code and region='North America' except select country.name,country.region,language,percentage from city,country,countrylanguage where countrylanguage.countrycode = country.code and city.countrycode = country.code and city.population<200000) Order by percentage DESC
# select country.name,country.region,language,percentage from country,countrylanguage where country.code = countrycode and language='English' and percentage>80.0 except select country.name,country.region,language,percentage from country,countrylanguage where countrycode=country.code and region='North America' Order by percentage DESC
conn = psycopg2.connect(database="test", user="pjy953", host="127.0.0.1", port="5432")

cur = conn.cursor()
text1 = "select country.name,city.name,country.population,city.population,countrylanguage.language "
text2 = "from city, country, countrylanguage "
text3 = "where "

conditions = []
conditions.append("city.countrycode = country.code")
conditions.append("countrylanguage.countrycode = country.code")
conditions.append("city.population>10000000")
conditions.append("city.name ='Japan'")

start = time.time()
for n in range(len(conditions)):
	text3 = text3 + conditions[n] + " and "
text3 = text3[0:len(text3)- 4]
text = text1 + text2 + text3
cur.execute(text)
rows = cur.fetchone()
if rows:
	print "answer"
	# for row in rows:
 #   		print "country.name = ", row[0]
 #   		print "city.name = ", row[1]
 #   		print "country.population = ", row[2]
 #   		print "city.population = ", row[3]
 #   		print "countrylanguage.language = ", row[4] , "\n"
else:
	print "no answer"

result = []
for L in range(1, len(conditions)+1):
    for subset in itertools.combinations(conditions, L):
    	text4 = "where "
    	listanswer = []
    	listanswer = listremove(subset,tuple(conditions))
    	for n in range(len(listanswer)):
			text4 = text4 + listanswer[n] + " and "
        if not text4 == "where ":
        	text4 = text4[0:len(text4)- 4]
        	query = text1 + text2 + text4 + " and country.name='China'"
        else:
        	query = text1 + text2 + text4 + " country.name='China'"
        cur.execute(query)
        rows = cur.fetchall()
        if rows:
        	result.append(subset)
    if result:
    	break;
print result
elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))

# tree2_1 = Node(" except ");
# tree2_2 = Node(" select distinct country.name from city,country ")
# tree2_3 = Node(" where ")
# tree2_4 = Node(" select distinct country.name from city,country ")
# tree2_5 = Node(" where ")
# tree2_6 = Node(1)
# tree2_7 = Node(-1)
# tree2_8 = Node(" city.countrycode = country.code ")
# tree2_9 = Node(" city.population>9000000" )
# tree2_10 = Node(" city.countrycode = country.code ")
# tree2_11 = Node(" city.population<90000 ")

# tree2_1.add_child(tree2_6)
# tree2_1.add_child(tree2_7)
# tree2_6.add_child(tree2_2)
# tree2_6.add_child(tree2_3)
# tree2_7.add_child(tree2_4)
# tree2_7.add_child(tree2_5)
# tree2_3.add_child(tree2_8)
# tree2_3.add_child(tree2_9)
# tree2_5.add_child(tree2_10)
# tree2_5.add_child(tree2_11)

# ground_data = "select distinct country.name from city,country where city.countrycode = country.code "

# subqueries = search(tree2_1)

# subtexts = []
# for i in range(len(subqueries)): # subqueries => subtexts
#     subtexts.append(traverse(subqueries[i].data))



# conditions2 = []
# conditions2.append("city.countrycode = country.code")
# conditions2.append("city.population>9000000")
# conditions3 = []
# conditions3.append("city.countrycode = country.code")
# conditions3.append("city.population<90000")
# #conditions3.append("country.name = 'China'")
# text1_example2 = "select distinct country.name from city,country "
# text2_example2 = "where "
# text3_example2 = "where "
# for n in range(len(conditions2)):
# 	text2_example2 = text2_example2 + conditions2[n] + " and "
# text2_example2 = text2_example2[0:len(text2_example2)- 4]
# for n in range(len(conditions3)):
# 	text3_example2 = text3_example2 + conditions3[n] + " and "
# text3_example2 = text3_example2[0:len(text3_example2)- 4]

# text_example2 = text1_example2 + text2_example2 + " except " + text1_example2 + text3_example2
# cur.execute(text_example2)
# rows = cur.fetchall()
# if rows:
# 	print "answer"
# else:
# 	print "no answer"

# result = []
# result_true = []
# for L in range(1, max(len(conditions3),len(conditions2))+1):
#     for subset in itertools.combinations(conditions2, L):
#     	text4 = "where "
#     	listanswer = []
#     	listanswer = listremove(subset,tuple(conditions2))
#     	for n in range(len(listanswer)):
# 			text4 = text4 + listanswer[n] + " and "
#         if not text4 == "where ":
#         	text4 = text4[0:len(text4)- 4]
#         	query = text1_example2 + text4 + " and country.name='China'" + " except " + text1_example2 + text3_example2
#         else:
#         	query = text1_example2 + text4 + " country.name='China'" + " except " + text1_example2 + text3_example2
#         cur.execute(query)
#         rows = cur.fetchall()
#         if rows:
#         	result.append(subset)
#         else:
#         	result_true.append(subset)

#     for subset in itertools.combinations(conditions3, L):
#     	text4 = "where "
#     	listanswer = []
#     	listanswer = listremove(subset,tuple(conditions3))
#     	for n in range(len(listanswer)):
# 			text4 = text4 + listanswer[n] + " and "
#         if not text4 == "where ":
#         	text4 = text4[0:len(text4)- 4]
#         	query = text1_example2 + text2_example2 +" except "+ text1_example2 + text4 + " and country.name!='China'"
#         else:
#         	query = text1_example2 + text2_example2 +" except "+ text1_example2 + text4 + " country.name!='China'"
#         cur.execute(query)
#         rows = cur.fetchall()
#         if rows:
#         	result.append(subset)
#     if result:
#     	break;

# for i in range(len(result_true)):
# 	if result_true[i] in result:
# 		result.remove(result_true[i])

# print result





# conditions4 = []
# conditions5 = []
# conditions6 = []

# text1_example3 = "select country.name,country.region,language,percentage from country,countrylanguage "
# text2_example3 = "select country.name,country.region,language,percentage from city,country,countrylanguage "
# text3_example3 = "where "
# text4_example3 = "where "
# text5_example3 = "where "

# conditions4.append("country.code = countrycode")
# conditions4.append("language='English'")
# conditions4.append("percentage>80.0")

# conditions5.append("countrycode=country.code")
# conditions5.append("region='North America'")

# conditions6.append("countrylanguage.countrycode = country.code")
# conditions6.append("city.countrycode = country.code")
# conditions6.append("city.population<50000")

# for n in range(len(conditions4)):
#     text3_example3 = text3_example3 + conditions4[n] + " and "
# text3_example3 = text3_example3[0:len(text3_example3)- 4]
# for n in range(len(conditions5)):
#     text4_example3 = text4_example3 + conditions5[n] + " and "
# text4_example3 = text4_example3[0:len(text4_example3)- 4]
# for n in range(len(conditions6)):
#     text5_example3 = text5_example3 + conditions6[n] + " and "
# text5_example3 = text5_example3[0:len(text5_example3)- 4]

# text_example3 = text1_example3 + text3_example3 + " except (" + text1_example3 + text4_example3 + " except " + text2_example3 + text5_example3 + ")"
# cur.execute(text_example3)
# rows = cur.fetchall()
# if rows:
#     print "answer"
# else:
#     print "no answer"

# result= []
# result_true = []

# for L in range(1, max(len(conditions4),len(conditions5),len(conditions6))+1):
#     for subset in itertools.combinations(conditions4, L):
#         text4 = "where "
#         listanswer = []
#         listanswer = listremove(subset,tuple(conditions4))
#         for n in range(len(listanswer)):
#             text4 = text4 + listanswer[n] + " and "
#         if not text4 == "where ":
#             text4 = text4[0:len(text4)- 4]
#             query = text1_example3 + text4 + "and country.name='Canada'"# + " except (" + text1_example3 + text4_example3 + " except " + text2_example3 + text5_example3+")"
#         else:
#             query = text1_example3 + text4 + "country.name='Canada'"# +  " except ("  + text1_example3 + text4_example3 + " except " + text2_example3 + text5_example3 + ")"
#         cur.execute(query)
#         rows = cur.fetchall()
#         if rows:
#             result.append(subset)
#         else:
#             result_true.append(subset)

#     for subset in itertools.combinations(conditions5, L):
#         text4 = "where "
#         listanswer = []
#         listanswer = listremove(subset,tuple(conditions5))
#         for n in range(len(listanswer)):
#             text4 = text4 + listanswer[n] + " and "
#         if not text4 == "where ":
#             text4 = text4[0:len(text4)- 4]
#             query = text1_example3 + text4 + "and country.name!='Canada'" #+ text1_example3 + text3_example3  +" except (" + " except " + text2_example3 + text5_example3+ ")"
#         else:
#             query = text1_example3 + text4 + "country.name!='Canada'" #+ text1_example3 + text3_example3+ " except (" + " except " + text2_example3 + text5_example3+ ")"
#         cur.execute(query)
#         rows = cur.fetchall()
#         if rows:
#             result.append(subset)
#     if result:
#         break;

#     for subset in itertools.combinations(conditions6, L):
#         text4 = "where "
#         listanswer = []
#         listanswer = listremove(subset,tuple(conditions6))
#         for n in range(len(listanswer)):
#             text4 = text4 + listanswer[n] + " and "
#         if not text4 == "where ":
#             text4 = text4[0:len(text4)- 4]
#             query = text2_example3 + text4 +"and country.name='Canada'"#text1_example3 + text3_example3+ " except (" + text1_example3 + text4_example3 + " except " + + ")"
#         else:
#             query = text2_example3 + text4 + "country.name='Canada'"#text1_example3 + text3_example3+ " except ("  + text1_example3 + text4_example3+ " except " + + ")"
#         cur.execute(query)
#         rows = cur.fetchall()
#         if rows:
#             result.append(subset)
#         else:
#             result_true.append(subset)

# for i in range(len(result_true)):
#     if result_true[i] in result:
#         result.remove(result_true[i])

# print result


# conn.close()




