# I would not import modules in this module, 
# in order to be a more straightforward file to write by an agronomist
from elaborator import *
from environment import *
from model import *
from rules import *

#This is what an agronomist or a developer should write to produce
#a model, an analysis or whatever.
Environment(test_var=999)

#Defining some variables and situations
Elaborator(test_var="Data")

#here we add the Model with the rules
Model(rules=[
    SimpleComparativeRule(column='rain', condition='gt0', is_implicit=True)
])



