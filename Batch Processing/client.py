
from celery import chord
from tasks import file_sum, word_count

import sys
import os

DIR=sys.argv[1]

abs_files = [os.path.join(pth, f) for pth, dirs, files in os.walk(DIR) for f in files]
length = len(abs_files)
divisor = length//21		#divisor is the batch size
if divisor == 0:
	divisor=1
a=length//divisor		#'a' is number of batches
l1=[]
i=0
for i in range(a):
	l1+=[abs_files[i*divisor:(i+1)*divisor]]
if a*divisor < length:
	l1+=[abs_files[(i+1)*divisor:length]]
x = chord([file_sum.s(filename) for filename in l1])(word_count.s()).get()
print(x)
"""
wc={}
for filename in abs_files:
	with open(filename, mode='r', newline='\r') as f:
		for text in f:
			if text == '\n':
				continue
			sp = text.split(',')[4:-2]
			tweet = " ".join(sp)
			for word in tweet.split(" "):
				if word not in wc:
					wc[word]=0
				wc[word] = wc[word]+1

if(x == wc):
	print("1")

"""
