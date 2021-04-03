###playA 对上 defenderB 的场数

import sys
from operator import itemgetter


###mapper1

for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	player = line[-2]
	defender = line[-8]+line[-7]
	

	print ('%s\t%s' % (player+defender,1))   


