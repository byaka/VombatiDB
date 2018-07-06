# -*- coding: utf-8 -*-
import sys
global PY_V
PY_V=float(sys.version[:3])

try:
   import cPickle as pickle
except ImportError:
   import pickle

sys.path.append('/var/python/libs/')
sys.path.append('/var/python/')
sys.path.append('/home/python/libs/')
sys.path.append('/home/python/')
from functionsex import *
timetime=time.time

class BaseDBError(Exception):
   """ Base Exception class fro other DB-specific errors. """

class BadLinkError(BaseDBError):
   """ Raise this if link referred to non-exists data. """

class StrictModeError(BaseDBError):
   """ Raise this only in strict mode for cases, where usually we shows warning. """

class ExtensionDependencyError(BaseDBError):
   """ Raise this if requested extensions have unmet dependencies. """

def statsFormat(data):
   speedTree, speedFlat=data['speedstatsTree'], data['speedstatsFlat']
   colName=('Method', 'N', 'min', 'mid', 'max', 'maxAll')
   colW=tuple(len(s) for s in colName)
   for name, val in speedFlat.iteritems():
      val=[time2human(s, inMS=False) if i else str(s) for i,s in enumerate(val)]
      if val[-1]==val[-2]: val[-1]='-'
      speedFlat[name]=tuple(val)
      colW=tuple(max(s, len(val[i-1])) if i else colW[0] for i,s in enumerate(colW))
      for ext, val in speedTree[name].iteritems():
         val=[time2human(s, inMS=False) if (i and not isStr(s)) else str(s) for i,s in enumerate(val)]
         if val[-1]==val[-2]: val[-1]='-'
         speedTree[name][ext]=tuple(val)
         colW=tuple(max(s, len(val[i-1])) if i else max(colW[0], len('%s  %s'%(name, ext))) for i,s in enumerate(colW))
   #
   res=[]
   res.append(' | '.join(s.center(colW[i]) for i,s in enumerate(colName))+'|')
   res.insert(0, '-'*len(res[0]))
   res.append('-'*len(res[0]))
   for name in sorted(speedTree):
      oo=(name,)+speedFlat[name]
      res.append(' | '.join(s.center(colW[i]) if i else s.ljust(colW[i]) for i,s in enumerate(oo))+'|')
      for ext in sorted(speedTree[name]):
         o=speedTree[name][ext]
         s=[ext.rjust(colW[0])]+[s.center(colW[i+1]) for i,s in enumerate(o)]
         res.append(' | '.join(s)+'|')
      res.append('-'*len(res[0]))
   res='\n'.join(res)
   return res
