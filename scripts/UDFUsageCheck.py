# -*- coding: utf-8 -*-
import argparse
import collections
import os
import random
import re
import subprocess
import sys

def fullSojPattern():
   return re.compile(r"[^a-zA-Z]([a-zA-Z]+\.soj_[^( ]+\([^)]+\))")

def shortSojPattern():
   return re.compile(r"[^a-zA-Z_]*([a-zA-Z0-9]+\.soj_[^( ]+)\(")

def sojOccur(input, searchType):
   (filePath,tmpFileName) = os.path.split(input)
   (fileName, ext) = os.path.splitext(tmpFileName)
   #print(fileName)
   with open(input, 'r') as f:
      content = f.read()
   func = "{st}Pattern".format(st=searchType)
   pattern = globals()[func]()
   methodInfo = pattern.findall(content)
   if (len(methodInfo)):
      d = dict()
      for method in methodInfo:
        if method in d:
          d[method] += 1
        else:
          d[method] = 1
      od = collections.OrderedDict(sorted(d.items()))
      ret = "{f}|{t}".format(f=fileName,t=len(methodInfo))
      for k, v in od.iteritems():
          ret = ret + "|{k}:{v}".format(k=k,v=v) 
      print(ret)

def usage():
   s='''
   -i <file>                     Specify the input file
   -t <fullSoj|shortSoj> Specify the search type: <fullSoj> includes lib, func, and args.
                                                  <shortSoj> includes lib, func
   -h                            Print this help
'''
   print(s)
    
if __name__=="__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("-i", "--input", help="Specify the input file")
   parser.add_argument("-t", "--type", choices=["fullSoj","shortSoj"],type=str, default="fullSoj", help="Specify which sojcall you want to find: 'fullSoj' or 'shortSoj'")
   args = parser.parse_args()
   if args.input is None:
      usage()
   else:
      sojOccur(args.input, args.type)
