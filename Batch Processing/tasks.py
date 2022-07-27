
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from collections import Counter

app = Celery('tasks', backend='redis://localhost:6379', broker='pyamqp://guest@localhost//', worker_prefetch_multiplier = 1)

@app.task(acks_late=True)
def file_sum(fname):
	wc= {}
	for i in fname:
		with open(i, mode='r', newline='\r') as f:
			for text in f:
				if text=='\n':
					continue
				sp = text.split(',')[4:-2]
				tweet = " ".join(sp)
				for word in tweet.split(" "):
					if word not in wc:
						wc[word]=0
					wc[word]=wc[word]+1
	return wc


@app.task(acks_late=True)
def word_count(files):
	count=Counter()
	for i in files:
		count.update(i)
	return dict(count)
