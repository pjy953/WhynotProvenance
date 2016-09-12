import itertools

stuff = ["D=H", "E=H", "H<800"] # Atoms
result = [] 
for L in range(1, len(stuff)+1):
    for subset in itertools.combinations(stuff, L):
        print stuff-subset
        for i in range(0, len(subset)):
	        if subset[i] == "f": 
	            result.insert(0,subset[0]) 
    if result:
        break;
# if result == False
#     return False