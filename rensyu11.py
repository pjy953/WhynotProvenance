# query except query
# why and why-not
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


conn = psycopg2.connect(database="test", user="pjy953", host="127.0.0.1", port="5432")

cur = conn.cursor()

conditions2 = []
conditions2.append("city.countrycode = country.code")
conditions2.append("city.population>9000000")
# conditions2.append("country.name = 'South Korea'")
conditions3 = []
conditions3.append("city.countrycode = country.code")
conditions3.append("city.population<90000")
conditions3.append("country.name = 'China'")
text1_example2 = "select distinct country.name from city,country "
text2_example2 = "where "
text3_example2 = "where "
for n in range(len(conditions2)):
    text2_example2 = text2_example2 + conditions2[n] + " and "
text2_example2 = text2_example2[0:len(text2_example2)- 4]
for n in range(len(conditions3)):
    text3_example2 = text3_example2 + conditions3[n] + " and "
text3_example2 = text3_example2[0:len(text3_example2)- 4]

text_example2 = text1_example2 + text2_example2 + " except " + text1_example2 + text3_example2
print text_example2
cur.execute(text_example2)
rows = cur.fetchall()
if rows:
    print "answer"
else:
    print "no answer"

result = []
result2 = []
result_true = []

subquery = []
subquery.append(text1_example2 + text2_example2 +" and country.name='China' and country.name!='South Korea'")
subquery.append(text1_example2 + text3_example2 +" and country.name='China' and country.name!='South Korea'")

start = time.time()

for i in range(len(subquery)):
    cur.execute(subquery[i])
    rows = cur.fetchall()
    if i == 1:
        if rows:
            print subquery[i]
            for L in range(1, max(len(conditions3),len(conditions2))+1):
                for subset in itertools.combinations(conditions3, L):
                    text4 = "where "
                    listanswer = []
                    listanswer = listremove(subset,tuple(conditions3))
                    for n in range(len(listanswer)):
                        text4 = text4 + listanswer[n] + " and "
                    if not text4 == "where ":
                        text4 = text4[0:len(text4)- 4]
                        query = text1_example2 + text4 + " and country.name!='China' and country.name='South Korea'"
                    else:
                        query = text1_example2 + text4 + " country.name!='China' and country.name='South Korea'"
                    cur.execute(query)
                    rows = cur.fetchall()
                    if rows:
                        result2.append(subset)
                    else:
                        result_true.append(subset)
                if result2:
                    break
        else:
            for L in range(1, max(len(conditions3),len(conditions2))+1):
                for subset in itertools.combinations(conditions3, L):
                    text4 = "where "
                    listanswer = []
                    listanswer = listremove(subset,tuple(conditions3))
                    for n in range(len(listanswer)):
                        text4 = text4 + listanswer[n] + " and "
                    if not text4 == "where ":
                        text4 = text4[0:len(text4)- 4]
                        query = text1_example2 + text4 + " and country.name='China' and country.name!='South Korea'" # != <-> =
                    else:
                        query = text1_example2 + text4 + " country.name='China' and country.name!='South Korea'"
                    print query
                    cur.execute(query)
                    rows = cur.fetchall()
                    if rows:
                        result2.append(subset)
                        result_true.append(subset)
                if result2:
                    break
    elif i == 0:
        if not rows:
            for L in range(1, max(len(conditions3),len(conditions2))+1):
                for subset in itertools.combinations(conditions2, L):
                    text4 = "where "
                    listanswer = []
                    listanswer = listremove(subset,tuple(conditions2))
                    for n in range(len(listanswer)):
                        text4 = text4 + listanswer[n] + " and "
                    if not text4 == "where ":
                        text4 = text4[0:len(text4)- 4]
                        query = text1_example2 + text4 + " and country.name='China' and country.name!='South Korea'"
                    else:
                        query = text1_example2 + text4 + " country.name='China' and country.name!='South Korea'"
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

result = result + result2

for i in range(len(result_true)):
    if result_true[i] in result:
        result.remove(result_true[i])

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time))

print result

conn.close()




