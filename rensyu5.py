from pyDatalog import pyDatalog
import itertools
pyDatalog.create_terms('atom1,atom2,atom3,result,book,apli,apli_book,A,B,C,D,E,F,G,H')
 
+ book('b1','Odyssey',15)
+ book('b2','illiad',45)
+ book('b3','antigone',49)

+ apli('a1','homer',800)
+ apli('a2','sophocles',400)
+ apli('a3','euripides',400)

+ apli_book('a1','b2')
+ apli_book('a1','b1')
+ apli_book('a2','b3')

Atoms = []
Atoms_literal = ["A==H","D==G","F<800"]
baseatom = book(A,B,C)&apli(D,E,F)&apli_book(G,H)
atom1 = (A==H)
atom2 = (D==G)
atom3 = (F<800)
Atoms.append(atom1)
Atoms.append(atom2)
Atoms.append(atom3)

for i in range(0,len(Atoms)):
	baseatom = baseatom & Atoms[i]

result(A,B,C) <= baseatom
print (result(A,B,C))

print(pyDatalog.ask('result(A,antigone,C)'))

result = [] 
for L in range(1, len(Atoms_literal)+1):
	query = baseatom & (E=='homer')
	print query
	for subset in itertools.combinations(Atoms_literal, L):
		baseatom2 = book(A,B,C)&apli(D,E,F)&apli_book(G,H)
		for m in range(0,len(subset)):
			number = Atoms_literal.index(subset[m])
			print number
			for i in range(0,len(subset)):
				if i != m:
					baseatom2 = baseatom2 & Atoms[m]
		print ('----')
			# query = query
			# print query
