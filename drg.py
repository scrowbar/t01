#!/usr/bin/env python

from struct import *
import os 
from datetime import datetime, timedelta
import time
import sqlite3


_DATA_DIR = '/data'
_WORK_DIR = '/base'
_BUF_SIZE = 76

def tmfilter(tmrec, tmfr, tmto):   
   if (tmfr != -1) and (tmto != -1):
      return ((tmrec >= tmfr) and (tmrec <= tmto))
   if tmfr != -1:
      return tmrec >= tmfr
   if tmto != -1:
      return tmrec <= tmto


def createDbFile(date):


   directory = _WORK_DIR + str(date.year) + "/" + str(date.month).zfill(2) + "/"
   if not os.path.exists(directory):
      os.makedirs(directory)

   filename = directory + str(date.date()) + ".db"
   if os.path.exists(filename):
      os.remove(filename)

   return filename




def collectFiles(from_time, to_time, dir):
   result = []
   found_timestamps  =[]
   i  = 0
   utm_prefix = "iptraffic_raw_"
   for root, dirs, files  in os.walk(dir):
      for fname in files:
         if utm_prefix in fname:
            ftime = int(fname[len(utm_prefix):-4])
            #print "from: %d, to: %d, cur: %s" % (from_time, to_time, ftime)
            found_timestamps.append(ftime)

   #Sorintg 
   for j in range(0, len(found_timestamps)):
      min_ts = found_timestamps[j]
      min_ts_pos = j
      for k in range(j, len(found_timestamps)):
         if found_timestamps[k] < min_ts:
            min_ts = found_timestamps[k]
            min_ts_pos = k

      tmp = found_timestamps[j]
      found_timestamps[j] = found_timestamps[min_ts_pos];
      found_timestamps[min_ts_pos] = tmp

   #Specified range
   if ((from_time != -1) and (to_time !=1)):
      for j in range(0, len(found_timestamps)):
         if (found_timestamps[j] >= from_time) and (found_timestamps[j] <= to_time):
            result.append(utm_prefix + str(found_timestamps[j]) + '.utm')
      in_range = 0      
      for j in range(len(found_timestamps) - 1, -1, -1):         
         if found_timestamps[j] <= to_time and in_range == 0: in_range = 1
         if found_timestamps[j] <= from_time and in_range == 1:
            result.insert(0, utm_prefix + str(found_timestamps[j]) + '.utm') 
            break



   #print result

   return result

date = fr = datetime.now() - timedelta(days=1)

to = fr 
fr = fr.replace(hour=0,minute=0,second=0,microsecond=0)
to = to.replace(hour=23,minute=59,second=59,microsecond=0)

fr = int(time.mktime(fr.timetuple()))
to = int(time.mktime(to.timetuple()))


files_to_parse = collectFiles(fr,to, _DATA_DIR)


if len(files_to_parse) > 0:
   

   conn = sqlite3.connect(createDbFile(date))
   c = conn.cursor()
   c.execute('''CREATE TABLE traffic
                  (timestamp INTEGER, acc_id INTEGER, s_addr INTEGER, d_addr INTEGER, pkts INTEGER, bytes INTEGER, 
                  s_port UNSIGNEDSHORT, d_port UNSIGNEDSHORT, proto UNSIGNEDCHAR                   
                  )''')
   conn.commit()

   for f in range(0, len(files_to_parse)):
      i = 0
      rec_num = 0
      print "Porcessing file: %s" % files_to_parse[f]
      with open(_DATA_DIR + files_to_parse[f],'rb') as fp:
 
            #fp.seek(size - 76*1000)
            while 1:
                  buf = fp.read(_BUF_SIZE)
                  if not buf:
                     break            

                  if len(buf) == _BUF_SIZE:
                     rec =  unpack_from('iiiiiiiiiHHHBBHHHHiiiiii',buf,0)            
                 
                  if not tmfilter(rec[22], fr, to):
                     continue
                  if rec[19] == 0:
                     continue

                  #query = 'insert into traffic values (%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i)' % tuple(tmp[x] for x in range(0,len(tmp)))
                  query = 'insert into traffic values (?,?,?,?,?,?,?,?,?)' 
                  c.execute(query, (rec[22], rec[19], rec[1], rec[2], rec[5], rec[6], rec[9], rec[10], rec[12]) )
                  #print tmp
                  i += 1

      conn.commit()
      print i 
   conn.close()
     









#print repr(buf)

"""
class iptraffic_raw_utm
{ public:
   int something1; // always 1
   int sourceaddr;
   int destaddr;
   int nexthop; // 0
   int something2; // 0x200
   int packets;
   int octets;
   int first;
   int last;
   unsigned short src_port;
   unsigned short dest_port;
   unsigned short tcp_flags;
   unsigned char proto;
   unsigned char tos;
   unsigned short something3;
   unsigned short src_as;
   unsigned short dst_as;
   unsigned short something4; // 0x7C34
   int slink_id;
   int account_id;
   int sourceaddr1;
   int t_class; // 0x3F2, 0x3EE
   int timestamp;
   int nfgen_addr;
 """