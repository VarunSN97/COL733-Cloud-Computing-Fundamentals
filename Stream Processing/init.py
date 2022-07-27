from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS
from tasks import feed, word_count, app

import time
## Configure consumer groups and workers

def setup_stream(stream_name: str):
  garbage = {"a": 1}
  # Create the stream by adding garbage values
  id = rds.xadd(stream_name, garbage)
  rds.xdel(stream_name, id)

rds.flushall()
setup_stream(TWEET)

for i in range(WORD_BUCKETS):
  stream_name = f"{WORD_PREFIX}{i}"
  setup_stream(stream_name)

obj = app.control.inspect()
count = 0
iterator=0
cel_count=0

s1=False
s2=False
s3=False

rds.xgroup_create(TWEET, "firstgroup")
rds.xgroup_create("w_0", "secondgroup")
rds.xgroup_create("w_1", "thirdgroup")
while (((s1 and s2 and s3) == False) or (count!=6)):	
	t_read = rds.xreadgroup("firstgroup", "c0", {TWEET : ">"},800)
	if t_read!=[]:
		a= t_read[0][1]
		feed.s(a).delay()
		s1 = False
		count=0
	elif t_read == []:
		s1 = True
	w0_read = rds.xreadgroup("secondgroup", "c01", {"w_0" : ">"},4000)
	w1_read = rds.xreadgroup("thirdgroup", "c02", {"w_1" : ">"},4000)
	if w0_read!=[] and w1_read!=[]:
		a = w0_read[0][1]
		g1 = "secondgroup"
		s_name = "w_0"
		word_count.s(a,g1,s_name).delay()
		b = w1_read[0][1]
		g2 = "thirdgroup"
		s_name = "w_1"
		word_count.s(b,g2,s_name).delay()
		s2=False
		s3=False
		count=0
	elif w0_read==[] and w1_read==[] :
		s2=True
		s3=True
	elif w0_read!=[]:
		a = w0_read[0][1]
		g1 = "secondgroup"
		s_name = "w_0"
		word_count.s(a,g1,s_name).delay()
		s2=False
		s3=True
		count=0
	else:
		a = w1_read[0][1]
		g1 = "thirdgroup"
		s_name = "w_1"
		word_count.s(a,g1,s_name).delay()
		s2 = True
		s3 = False
		count=0

	if s1 and s2 and s3 == True:
		count+=1
		time.sleep(5)

while True:
	time.sleep(3)
	alpha = len(list(obj.scheduled().values())[0])
	beta = len(list(obj.reserved().values())[0])
	gamma = len(list(obj.active().values())[0])
	if alpha + beta + gamma <=0:
		cel_count+=1
	else:
		cel_count=0
	if cel_count==4:
		break


k1=False
k2=False
k3=False
nextid1="0"
nextid2="0"
nextid3="0"
while (k1 and k2 and k3) == False:
	temp_read = rds.xreadgroup("firstgroup", "c0", {TWEET : nextid1}, 1000)
	temp_read2 = rds.xreadgroup("secondgroup", "c01", {"w_0" :nextid2},4000)
	temp_read3 = rds.xreadgroup("thirdgroup", "c02", {"w_1": nextid3},4000)
	

	if temp_read[0][1]!=[]:
		feed.s(temp_read[0][1]).delay()
		nextid1 = temp_read[0][1][len(temp_read[0][1])-1][0]
		k1 = False
	else:
		k1 =True

	if temp_read2[0][1]!=[] and temp_read3[0][1]!=[]:
		word_count.s(temp_read2[0][1], "secondgroup", "w_0").delay()
		word_count.s(temp_read3[0][1], "thirdgroup", "w_1").delay()
		nextid2 = temp_read2[0][1][len(temp_read2[0][1])-1][0]
		nextid3 = temp_read3[0][1][len(temp_read3[0][1])-1][0]
		k2 = False
		k3 =False
	elif temp_read3[0][1]!=[]:
		word_count.s(temp_read3[0][1], "thirdgroup", "w_1").delay()
		nextid3 = temp_read3[0][1][len(temp_read3[0][1])-1][0]
		k3 = False
		k2 =True
	elif temp_read2[0][1]!=[]:
		word_count.s(temp_read2[0][1], "secondgroup", "w_0").delay()
		nextid2 = temp_read2[0][1][len(temp_read2[0][1])-1][0]
		k2 =False
		k3 =True
	else:
		k2=True
		k3=True
		

temporary= rds.xreadgroup("secondgroup","c01",{"w_0":">"},4000)
if temporary !=[]:
	word_count.s(temporary[0][1],"secondgroup","w_0").delay()

temporary2 = rds.xreadgroup("thirdgroup","c02",{"w_1":">"},4000)
if temporary2 !=[]:
	word_count.s(temporary2[0][1],"thirdgroup","w_1").delay()


