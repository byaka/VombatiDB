# -*- coding: utf-8 -*-
import sys, unicodedata
global PY_V
PY_V=float(sys.version[:3])

try:
   import cPickle as pickle
except ImportError:
   import pickle

try:
   from lru import LRU as lruDict
except ImportError:
   global lruDict
   lruDict=False

sys.path.append('/var/python/libs/')
sys.path.append('/var/python/')
sys.path.append('/home/python/libs/')
sys.path.append('/home/python/')

try:
   from functionsex import *
except ImportError:
   from functionsEx.functionsex import *
from gevent.lock import RLock

class BaseDBError(Exception):
   __slots__=()
   pass

class BaseDBErrorPrefixed(BaseDBError):
   """Something goes wrong in VambatiDB"""
   __slots__=('msg',)
   def __init__(self, msg=None, useDefPrefix=True):
      if msg and useDefPrefix:
         msg='%s: %s'%(self.__class__.__doc__, msg)
      else:
         msg=msg or self.__class__.__doc__
      super(BaseDBError, self).__init__(msg)

class BadIdError(BaseDBErrorPrefixed):
   """Object's id contains forbidden symbols"""

class BadLinkError(BaseDBErrorPrefixed):
   """Referring to non-existed object"""

class StrictModeError(BaseDBError):
   """Raise this only in strict mode for cases, where usually we shows warning"""

class ParentNotExistError(BaseDBErrorPrefixed, StrictModeError):
   """Parent not exists"""

class ExistStatusMismatchError(BaseDBErrorPrefixed, StrictModeError):
   """Exist-status mismatching"""

class NotExistError(BaseDBErrorPrefixed, StrictModeError):
   """Object not exists"""

class ExtensionDependencyError(BaseDBErrorPrefixed):
   """Unmet dependencies for extension"""

class BadPatternSpecial(object):
   __slots__=('value', 'setts')
   def __init__(self, value, **setts):
      self.value=value
      self.setts=setts
class BadPatternStarts(BadPatternSpecial): pass
class BadPatternEnds(BadPatternSpecial): pass
class BadPatternRE(BadPatternSpecial):
   __slots__=('flags',)
   def __init__(self, value, flags=0, **setts):
      super(BadPatternRE, self).__init__(value, **setts)
      self.flags=flags
class BadPatternREMatch(BadPatternRE): pass
class BadPatternRESearch(BadPatternRE): pass

def showStats(db):
   if getattr(db, '_destroyed', False): return
   data=db.stats()
   colors=console.color
   inTerm=console.inTerm()
   if not inTerm:
      colors={k:'' for k in colors}
   sepV=u' │ '
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
         colW=tuple(max(s, len(val[i-1])+1) if i else max(colW[0], len('%s   %s'%(name, ext))) for i,s in enumerate(colW))
   # printing
   res=[]
   sepHT, sepH, sepHB=[], [], []
   l=len(colW)-1
   for i, w in enumerate(colW):
      s=u'─'*(w+2 if i else w)
      sepHT.append((u'┌' if not i else '')+s+(u'┬' if i<l else u'┐'))
      sepH.append( (u'├' if not i else '')+s+(u'┼' if i<l else u'┤'))
      sepHB.append((u'└' if not i else '')+s+(u'┴' if i<l else u'┘'))
   sepHT, sepH, sepHB=''.join(sepHT), ''.join(sepH), ''.join(sepHB)
   res.append(sepHT)
   res.append(u'│'+(sepV.join(s.center(colW[i]-int(not(i))) for i,s in enumerate(colName))+sepV))
   for name in sorted(speedTree):
      res.append(sepH)
      oo=(u'│'+name,)+speedFlat[name]
      res.append(sepV.join(s.center(colW[i]) if i else s.ljust(colW[i]) for i,s in enumerate(oo))+sepV)
      for ext in sorted(speedTree[name]):
         o=speedTree[name][ext]
         s=[u'│'+unicode(ext.rjust(colW[0]-1))]+[unicode(s.center(colW[i+1])) for i,s in enumerate(o)]
         res.append(sepV.join(s)+sepV)
   res.append(sepHB)
   res=u'\n'.join(res)
   print res

def showDB(db, branch=None, limit=None):
   if getattr(db, '_destroyed', False): return
   # if not db.supports.get('query'):
   #    raise NotImplementedError('For special queries extension `DBSearch*` needed')
   colors=console.color
   inTerm=console.inTerm()
   width=console.width()
   if not inTerm:
      colors={k:'' for k in colors}
   sep=u'─'*(width if inTerm else 50)
   print sep
   i=0
   for ids, (props, branchLen) in db.iterBranch(branch, treeMode=True, safeMode=False, calcProperties=True, skipLinkChecking=True):
      if limit and i==limit:
         print '%(inverse)s%(light)s... SOME MORE OBJECTS HIDDEN ...%(end)s'%colors
         break
      lvl=len(ids)-1
      data=db.get(ids, existChecked=props)
      _data=repr(data) if data is not None else 'REMOVED'
      _props=repr(props)
      o={
         'indent':('  '*(lvl-1)+u'∟ ') if lvl else u'',
         'ids':ids[-1],
         'data':_data,
         'props':_props,
         'indent2':'',
      }
      if inTerm:
         leftPart='%(indent)s%(ids)s '%dict(colors, **o)
         maxSizeLeftPart=int(width*0.4)
         # обрезаем слишком длинные айдишники
         if len(leftPart)>maxSizeLeftPart:
            l=(maxSizeLeftPart-len(o['indent'])-1-3)/2
            o['ids']=o['ids'][:l]+'...'+o['ids'][-l:]
            leftPart='%(indent)s%(ids)s '%dict(colors, **o)
         #
         leftPartLen=len(leftPart)
         o['indent3']=' '*leftPartLen
         # перенос `data` с отступами, если она не помещается в одной строке
         l=leftPartLen
         tArr=[]
         while l<width and _data and l+len(_data)>width:
            l2=width-l
            tArr.append(_data[:l2])
            _data=_data[l2:]
         if tArr and _data:
            tArr.append(_data)
         o['data']=('%(end)s%(indent3)s%(inverse)s%(light)s'%dict(colors, **o)).join(tArr) if tArr else _data
         _dataLastLine='%(indent3)s'%o+tArr[-1] if tArr else leftPart+_data
         # выравнивание `props` по правому краю
         maxSizeProps=min(50, len(_props))
         if len(_dataLastLine)+1+len(_props)>width:
            if width-len(_dataLastLine)-1<maxSizeProps:
               o['indent2']=' '*(width-len(_dataLastLine)-1)+o['indent3']
               l=leftPartLen
            else:
               l=len(_dataLastLine)+1
               o['indent3']=' '*l
            tArr=[]
            while l<width and _props and l+len(_props)>width:
               l2=width-l
               tArr.append(_props[:l2])
               _props=_props[l2:]
            if tArr and _props:
               tArr.append(_props)
            o['props']=('%(end)s%(indent3)s%(light)s'%dict(colors, **o)).join(tArr) if tArr else _props
         else:
            o['indent2']=' '*(width-len(_dataLastLine)-len(_props)-1)
      msg='%(indent)s%(bold)s%(ids)s%(end)s %(inverse)s%(light)s%(data)s%(end)s %(indent2)s%(light)s%(props)s%(end)s'%dict(colors, **o)
      print msg
      i+=1
   print sep
