'''
Created on Feb 1, 2013

@author: ntilmans
'''



internalPhenols = frozenset([8,104,128,152,176,    77,    25,26,186])

lister = [13,8,101,301]

print len(set(lister).difference(internalPhenols))


internalPhenols = frozenset([8,104,128,152,176,    77,    25,26,186])
terminalPhenols = frozenset([211,224,236,248,    270,    281])
allPhenols = internalPhenols.union(terminalPhenols)

print allPhenols
'''
dicter = {1:"4", 3:"4", 5:"6"}

test


print dicter.values()
'''




'''
import re
import pandas as pd

readin = pd.read_table("testinput",delimiter='\t', index_col=0)

print readin


for entry in readin:
    print entry
'''
'''
from string import maketrans

def calcGC( inputSeq ):
    counter = 0
    
    inputSeq = inputSeq.strip().lower()
    for letter in inputSeq:
        if letter == 'g' or letter == 'c':
            counter += 1
            
    return float(counter)/float(len(inputSeq))


def complement( inputSeq ):
    inputSeq = inputSeq.strip().lower()
    
    inputone = 'acgt'
    midone = '1234'
    outone = 'tgca'
    
    stepone = maketrans(inputone, outone)
    #steptwo = string.maketrans(midone, outone)
    
    
    inputSeq = inputSeq.translate(stepone)
    
    return inputSeq

print complement('acgtttacggt')
print calcGC('gcgcattaaaa')
'''
'''
atomre = re.compile("^ATOM")

line = "ATOM    452  O   ASN A 732     144.197  38.715  46.964  1.00 44.67           O"
print line
tempfactor = 0.87562
liner = ()
if atomre.search(line.strip()):
    liner = list(line)
    print liner[23:26]
    liner[55:60] = list(str(" "+str(tempfactor)[0:4]))
    
lining = "".join(liner)

print lining

'''