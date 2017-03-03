from pyDatalog import pyDatalog

class Book(pyDatalog.Mixin):   # --> Employee inherits the pyDatalog capability to use logic clauses
    
    def __init__(self, number, name, price): # method to initialize Employee instances
        super(Book, self).__init__() # calls the initialization method of the Mixin class
        self.number = number
        self.name = name           # direct manager of the employee, or None
        self.price = price             # monthly salary of the employee
    
    def __repr__(self): # specifies how to display an Employee
        return self.number
        return self.name
        return self.price

class Apli(pyDatalog.Mixin):   # --> Employee inherits the pyDatalog capability to use logic clauses
    
    def __init__(self, number, name, year): # method to initialize Employee instances
        super(Apli, self).__init__() # calls the initialization method of the Mixin class
        self.number = number
        self.name = name           # direct manager of the employee, or None
        self.year = year             # monthly salary of the employee
    
    def __repr__(self): # specifies how to display an Employee
        return self.number

class Apli_book(pyDatalog.Mixin):   # --> Employee inherits the pyDatalog capability to use logic clauses
    
    def __init__(self, number1, number2,number3): # method to initialize Employee instances
        super(Apli_book, self).__init__() # calls the initialization method of the Mixin class
        self.number1 = number1
        self.number2 = number2       # direct manager of the employee, or None
        self.number3 = number3
    def __repr__(self): # specifies how to display an Employee
        return self.cid


b1 = Book('b1','Odyssey',15)
b2 = Book('b2','illiad',45)
b3 = Book('b3','antigone',49)

a1 = Apli('a1','homer',800)
a2 = Apli('a2','sophocles',400)
a3 = Apli('a3','euripides',400)

c1 = Apli_book('a1','b2','c1')
c2 = Apli_book('a1','b1','c2')
c3 = Apli_book('a2','b3','c3')

X= pyDatalog.Variable()
Book.price[X] > 20
print()