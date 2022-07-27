from celery import Celery
from config import rds, TWEET, WORD_PREFIX, WORDSET
import redis
import time
app = Celery('tasks', backend='redis://localhost:6579', broker='pyamqp://guest@localhost//',worker_prefetch_multiplier=1)



@app.task
def feed(tweetlist):
	p = rds.pipeline()
	for w in tweetlist:
		tweet=w[1]['tweet']
		if tweet == '\n':
			rds.xack(TWEET, "firstgroup", w[0])
			continue
		sp = tweet.split(',')[4:-2]
		text = " ".join(sp)
		p.multi()
		for word in text.split(" "):
			value = hash(word)%2
			s = f"{WORD_PREFIX}{value}"
			p.xadd(s, {word : 1})
		p.xack(TWEET, "firstgroup", w[0])
		p.execute()
			
	


@app.task
def word_count(wordlist, g, str):
	wc={}
	ic={}
	for i in wordlist:
		word = i[1]
		ack = [i[0]]
		for k in word.keys():
			if k not in wc:
				wc[k]= 0
			wc[k] = wc[k] + 1
			if k not in ic:
				ic[k]=ack
			else:
				ic[k]=ic[k]+ack

	p =rds.pipeline()
	while True:
		try:
			p.watch(WORDSET)
			p.multi()
			for f in wc.keys():
				p.zincrby(WORDSET, wc[f], f)
				for i in ic[f]:
					p.xack(str, g, i)
			p.execute()
			p.unwatch()
			break
		except redis.WatchError:
			pass

	  
