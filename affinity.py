import os
import re



PATH_SYS_SYSTEM = "/sys/devices/system";
PATH_SYS_CPU0   = PATH_SYS_SYSTEM + "/cpu/cpu0";
#print proc_num

def irqinfo(ifname):
	re_line = re.compile("\s*(\d+):\s+.*\s+"+ifname)
	#re_ifname = re.compile("("+ifname+")")
	irq_map = {}
	with open("/proc/interrupts",'r') as fp:
		for line in fp:
			#print line
			re_match = re_line.search(line)
			if re_match:
				cols = line.split()				
				irq_map[cols[-1]] = int(re_match.groups()[0])
	return irq_map

"""
def hweight(mask):
	count = 0
	return bin(mask).count('1')

def path_sibling(path):
	result = 0
	with open(path,'r') as fp:
		line = fp.readline().rstrip()
	for mask in line.split(','):
		result += hweight(int(mask,16));
	return result
"""

def cpuinfo():
	cpu = 0
	cpu_map = {}
	while (os.path.exists(PATH_SYS_SYSTEM + '/cpu/cpu' + str(cpu))):
		with open(PATH_SYS_SYSTEM + '/cpu/cpu'+str(cpu)+'/topology/physical_package_id') as fp:
			socket_num = int(fp.read().strip())
		with open(PATH_SYS_SYSTEM + '/cpu/cpu'+str(cpu)+'/topology/core_id') as fp:
			core_id = int(fp.read().strip())

		if not socket_num in  cpu_map:
			cpu_map[socket_num] = {}
		if not core_id in cpu_map[socket_num]:
			cpu_map[socket_num][core_id] = []

		cpu_map[socket_num][core_id].append(cpu)
		cpu += 1

	"""
	threads = path_sibling(PATH_SYS_CPU0 + '/topology/thread_siblings')
	cores   = path_sibling(PATH_SYS_CPU0 + '/topology/core_siblings') / threads
	sockets = cpu / cores / threads
	"""

	return cpu_map


def set_affinity(ifname,irq,mask):
	smp_affinity = "/proc/irq/"+str(irq)+"/smp_affinity";
	print "%s: irq %d affinity set to 0x%x" % (ifname, irq, mask);
	with open(smp_affinity,'w') as fp:
		fp.write("%x" % mask)


def set_single(ifname,irq):
	#TO DO: set rps in case of single queue with smp
	print "Only one queue availible. TODO set rps()"

def set_multi(ifname,irq_map):
	cpu_map = cpuinfo()
	socket,thread = choose_cpu(ifname,cpu_map)
	queues = {}
	for name in irq_map:
		if re.match("^[a-z]+\d+-\S+-\d+$",name):
			queues[name] = irq_map[name]
	if len(queues) > len(cpu_map[socket]):
		print "Not enough cores!  Queus: %d, cores for Socket %d: %d " % (len(queues), cpu, len(cpu_map[cpu]))
		return

	cur_core = 0
	#thread = choose_thread(ifname, len)
	for name in queues:
		#debug info
		print "CPU: %d (socket: %d, core: %d, thread: %d)" % (cpu_map[socket][cur_core][thread],socket,cur_core,thread)
		set_affinity(ifname,queues[name], 1 << cpu_map[socket][cur_core][thread])
		cur_core += 1


def auto_affinity(ifname):
	irq_map = irqinfo(ifname)
	if len(irq_map) == 1:
		set_single(ifname,irq_map[ifname])
	else:
		set_multi(ifname,irq_map)

def choose_cpu(ifname,cpu_map):

	ifunit = 0
	ifunit = re.search("^[a-z]+(\d+)",ifname)
	if not ifunit:		
		return 0
	ifunit = int(ifunit.groups()[0])
	#print ifunit
	sockets = len(cpu_map)
	threads = len(cpu_map[0][0])
	thread = (ifunit / sockets ) % threads
	socket = ifunit % sockets
	return  [socket,thread]

def usage():
	print """Usage: affinity.py [iface]
Example: affinity.py eth0"""

def main():
	import sys
	if len (sys.argv) < 2:
		usage()
		return 1
	ifname = sys.argv[1]
	if not os.path.exists("/sys/class/net/"+ifname):
		print "Interface " + ifname + " does not exist"
		return 1

	cpu_info = cpuinfo()
	print "cpus=%d cores=%d threads=%d sockets=%d" % (len(cpu_info[0])*len(cpu_info[0][0]), len(cpu_info[0]),len(cpu_info[0][0]), len(cpu_info) )
	auto_affinity(sys.argv[1])
	return 0

if __name__ == '__main__':
	main()