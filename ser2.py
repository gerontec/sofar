#!/usr/bin/python3
import serial
import os
import subprocess
import time
import datetime
from suntime import Sun, SunTimeException
now = datetime.datetime.utcnow()
print(str(now))
#ser = serial.Serial('/dev/ttyUSB40', 9600, timeout=33) #ttyUSB2
hx=""
#s = ser.read(512)
#ser.close()
#s = ""
#hx = s.hex()
#mystr = str(hx)
#substring = "7e7e02"
#i=6
#slen = len(mystr)
#print("Stringlength: " + str(slen))
#print(mystr)
cmd = "/home/pi/perl/killall.pl"
subprocess.call(["/usr/bin/perl",cmd,"fox2db"])
cmd2 = "/home/pi/perl/fox2db.pl"
cmd3 = "/home/pi/python/fox2db.py"
cmd4 = "/usr/local/bin/fox2db"
cmd5 = "/home/pi/python/fox2dbOO.py"
##subprocess.call(["/usr/bin/perl",cmd2,"arg2"])
#subprocess.call(["/usr/bin/python3",cmd3,"arg2"])
#subprocess.call(cmd4)  # C-Binary (auskommentiert für OO-Test)
subprocess.call(["/usr/bin/python3", cmd5])
