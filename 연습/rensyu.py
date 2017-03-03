# import itertools
# N = ["a","b","c","d","e"]
# print N[0]
# for i in range(1, 6):
#     for tuple in itertools.combinations(5, i):
#         print(tuple)
#         X = False
#         if tuple[0] == 'c':
#             X = True
#         if X == True:
#         	break;

import itertools

stuff = ["D=H", "E=H", "H<800"] # Atoms
result = [] #結果が保存されるところ
for L in range(1, len(stuff)+1):#Atomsの数くらい繰り返す
    for subset in itertools.combinations(stuff, L):#combinationを作る。combinations(stuff,L)は stuff_C_L を意味する。
        print(subset) #今回は結果の確認のために追加したもの。
        for i in range(0, len(subset)):#trueを満足するか確認する。
	        if subset[i] == "b" or subset[i] =="c": #trueかどうか判断する。今回はbかcか
	            result.insert(0,subset[0]) #trueであれば追加する。
    if result:#もし結果が存在すれば終了
        break;
print result

# Q1 <- Query, result =[]
# A1 <- atoms of Q1 (atoms except EDB or atoms including EDB)
# for i in range(1, len(A1)+1):
# 	for subset in itertools.combinations(stuff, i):
# 		for i in len(subset):
# 			Q2 <- Q1-subset[i]
# 			if Q2 == true:
# 				result = result + subset[i]
# 	if result:
# 		break;