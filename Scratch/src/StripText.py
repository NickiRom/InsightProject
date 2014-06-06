#!/usr/bin/python

'''
Created on Jun 5, 2014

@author: ntilmans
'''

import string


s = input('')
s = s.replace('\n',' ')
s = s.replace('\r',' ')

exclude = set(string.punctuation)
s = ''.join(ch for ch in s if ch not in exclude)

print s