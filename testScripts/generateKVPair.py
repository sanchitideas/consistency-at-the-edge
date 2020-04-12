import string
import random
import sys
import redis



def random_generator(size=1048576-49, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


redisInstance = redis.Redis(host='localhost', port=6379, db=0)

#number_of_kvpairs = 100
number_of_kvpairs = 1600000
value_size_bytes = 4*1024 - 49
for i in range(1, number_of_kvpairs+1):
    s = str(i)
    len_s = len(s)
    x = 128 -49 - len_s
    zeroes = '0'*x
    key = zeroes+s
    value = random_generator(value_size_bytes)
    #print(key)  
    redisInstance.set(key, value)

