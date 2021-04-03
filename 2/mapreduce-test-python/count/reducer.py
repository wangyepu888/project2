# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 20:22:10 2021

@author: Yepu Wang
"""
import sys
from operator import itemgetter
from collections import defaultdict



nums = {}

for line in sys.stdin:
	line = line.strip()
	player,num = line.split('\t')
	try:
		num=int(num)
		nums[player]= nums.get(player,0) + num
	except ValueError:
		pass

sorted_count = sorted(nums.items(),key=itemgetter(0),reverse=True)
for player, count in sorted_count:
	
	print ('%s\t%s' % (player,count))