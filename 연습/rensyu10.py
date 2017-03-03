#query except query2
#query2 : query except query

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
	# for i in range(0, len(mysubset)):
	# 	conditions.pop(mysubset[i])


conn = psycopg2.connect(database="test", user="pjy953", host="127.0.0.1", port="5432")

cur = conn.cursor()

conditions4 = []
conditions5 = []
conditions6 = []

text1_example3 = "select country.name,country.region,language,percentage from country,countrylanguage "
text2_example3 = "select country.name,country.region,language,percentage from city,country,countrylanguage "
text3_example3 = "where "
text4_example3 = "where "
text5_example3 = "where "

conditions4.append("country.code = countrycode")
conditions4.append("language='English'")
conditions4.append("percentage>80.0")

conditions5.append("countrycode=country.code")
conditions5.append("region='North America'")

conditions6.append("countrylanguage.countrycode = country.code")
conditions6.append("city.countrycode = country.code")
conditions6.append("city.population<50000")

for n in range(len(conditions4)):
    text3_example3 = text3_example3 + conditions4[n] + " and "
text3_example3 = text3_example3[0:len(text3_example3)- 4]
for n in range(len(conditions5)):
    text4_example3 = text4_example3 + conditions5[n] + " and "
text4_example3 = text4_example3[0:len(text4_example3)- 4]
for n in range(len(conditions6)):
    text5_example3 = text5_example3 + conditions6[n] + " and "
text5_example3 = text5_example3[0:len(text5_example3)- 4]

text_example3 = text1_example3 + text3_example3 + " except (" + text1_example3 + text4_example3 + " except " + text2_example3 + text5_example3 + ")"
cur.execute(text_example3)
rows = cur.fetchall()
if rows:
    print "answer"
else:
    print "no answer"

result= []
result_true = []

subsubquery = []
subquery = []
subsubquery.append(text1_example3 + text4_example3 + "and country.name='Canada'")
subsubquery.append(text2_example3 + text5_example3 + "and country.name='Canada'")

subquery.append(text1_example3 + text3_example3 + "and country.name='Canada'")
subquery.append(text1_example3 + text4_example3 + "and country.name='Canada'" + " except " + text2_example3 + text5_example3 + "and country.name='Canada'")

for i in range(len(subquery)):
    cur.execute(subquery[i])
    rows = cur.fetchall()
    if i == 1:
        for m in range(len(subsubquery)):
            if i == 0:
                for L in range(1, max(len(conditions3),len(conditions2))+1):
                    for subset in itertools.combinations(conditions3, L):
                        text4 = "where "
                        listanswer = []
                        listanswer = listremove(subset,tuple(conditions3))
                        for n in range(len(listanswer)):
                            text4 = text4 + listanswer[n] + " and "
                        if not text4 == "where ":
                            text4 = text4[0:len(text4)- 4]
                            query = text1_example2 + text4 + " and country.name='China'"
                        else:
                            query = text1_example2 + text4 + " country.name='China'"
                        cur.execute(query)
                        rows = cur.fetchall()
                        if rows:
                            result.append(subset)
                    if result:
                        break
            elif i == 1:

    elif i == 0:
        print 0
        for L in range(1, max(len(conditions4),len(conditions5),len(conditions6))+1):
            for subset in itertools.combinations(conditions4, L):
                text4 = "where "
                listanswer = []
                listanswer = listremove(subset,tuple(conditions4))
                for n in range(len(listanswer)):
                    text4 = text4 + listanswer[n] + " and "
                if not text4 == "where ":
                    text4 = text4[0:len(text4)- 4]
                    query = text1_example3 + text4 + "and country.name='Canada'"
                else:
                    query = text1_example3 + text4 + "and country.name='Canada'"
                cur.execute(query)
                rows = cur.fetchall()
                if rows:
                    result.append(subset)
                else:
                    result_true.append(subset)
            if result:
                break

for i in range(len(result_true)):
    if result_true[i] in result:
        result.remove(result_true[i])

print result


conn.close()




