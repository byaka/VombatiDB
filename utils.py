# -*- coding: utf-8 -*-
__ver_major__ = 0
__ver_minor__ = 2
__ver_patch__ = 0
__ver_sub__ = "dev"
__version__ = "%d.%d.%d" % (__ver_major__, __ver_minor__, __ver_patch__)
"""
:authors: John Byaka
:copyright: Copyright 2019, BYaka
:license: Apache License 2.0

:license:

   Copyright 2019 BYaka

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

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

try:
   from functionsex import *
except ImportError:
   from functionsEx.functionsex import *

import errors
from .errors import *

IN_TERM=consoleIsTerminal()
COLORS=MagicDict({k:(v if IN_TERM else '') for k,v in consoleColor.iteritems()})

""" Special classes for wrapping config of bad-patterns. """

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

class Workspace(object):
   __logLevelTemplate={
      0:'%(bold)s%(red)sERR ',
      1:'%(red)sERR ',
      2:'%(yellow)sWARN',
      3:'    ',
      4:'%(light)s    ',
   }

   def __init__(self, thread=None, sleep=None, input=None, rlock=None, fileWrap=None, log=None, **kwargs):
      if not thread or not rlock:
         import threading
         self.__threading=threading
      #
      self.thread=thread or self.__thread
      self.sleep=sleep or time.sleep
      self.raw_input=input or raw_input
      self.rlock=rlock or self.__threading.RLock
      self.fileWrap=fileWrap or (lambda f: f)
      self.log=log or self.__log
      #
      for k, v in kwargs.iteritems():
         setattr(self, k, v)

   def __thread(self, target, args=None, kwargs=None):
      t=self.__threading.Thread(target=target, args=args or (), kwargs=kwargs or {})
      t.daemon=True
      t.start()
      return t

   def __log(self, levelId, *args):
      try:
         t=timetime()
         if levelId not in self.__logLevelTemplate:
            args=(levelId,)+args
            lvl=self.__logLevelTemplate[3]
         else:
            lvl=self.__logLevelTemplate[levelId]
         data=[]
         for x in args:
            if not isString(x):
               try: x=str(x)
               except Exception:
                  try: x=self._server._serializeJSON(x)
                  except Exception:
                     try: x=repr(x)
                     except Exception: x='{UNPRINTABLE}'
            data.append(x)
         data=' '.join(data)
         caller=selfInfo(-3)
         if '/' in caller.module:
            caller.module=getScriptName(True, f=caller.module)
         caller.name='<%(module)s>.%(name)s()'%caller if caller.name!='<module>' else '<%s>'%caller.module
         caller='%(name)s:%(line)s'%caller
         #
         p=COLORS.copy()
         p['level']=lvl%p
         p['caller']=caller
         p['timestamp']=datetime.datetime.fromtimestamp(t).strftime('%m-%d_%H:%M:%S')
         p['data']=data
         s='%(light)s%(timestamp)s%(end)s %(level)s[%(caller)s] %(data)s%(end)s'%p
         try:
            print s
         except UnicodeEncodeError:
            print decode_utf8(s)
      except Exception:
         print '!!! Error in Logger(%s: %r): %s'%(levelId, args, getErrorInfo())

class WorkspaceOld(Workspace):
   def __init__(self, workspace, **kwargs):
      if workspace:
         if hasattr(workspace, 'server') and hasattr(workspace, 'log'):
            tArr={
               'thread':getattr(workspace.server, '_thread'),
               'sleep':getattr(workspace.server, '_sleep'),
               'input':getattr(workspace.server, '_raw_input'),
               'fileWrap':getattr(workspace.server, '_fileObj'),
               'log':getattr(workspace, 'log'),
            }
            if workspace.server.settings.gevent:
               from gevent.lock import RLock
            else:
               from threading import RLock
            tArr['rlock']=RLock
            _iter=workspace if isinstance(workspace, dict) else dir(workspace)
            for k in _iter:
               if k=='log': continue
               tArr[k]=oGet(workspace, k)
            #
            kwargs=dict(tArr, **kwargs)
         else:
            raise NotImplementedError("This workspace's format not supported")
      super(WorkspaceOld, self).__init__(**kwargs)

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
      data=db.get(ids, existChecked=props, returnRaw=True)
      if data is None:
         _data, _dataColor='REMOVED', 'red'
      else:
         _data=repr(data)
         _dataColor='yellow' if db.isLink(props) else 'light'
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
         o['data']=(('%(end)s%(indent3)s%(inverse)s%('+_dataColor+')s')%dict(colors, **o)).join(tArr) if tArr else _data
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
      msg=('%(indent)s%(bold)s%(ids)s%(end)s %(inverse)s%('+_dataColor+')s%(data)s%(end)s %(indent2)s%(light)s%(props)s%(end)s')%dict(colors, **o)
      print msg
      i+=1
   print sep

def dumpDB(db, branch=None):
   #! нужно добавить сравнение `meta` и `props`
   tArr={'branch':{}}
   if branch:
      tArr['branch'][db._prepIds(branch)]={}
   for ids, (props, branchLen) in db.iterBranch(branch, treeMode=True, safeMode=False, calcProperties=False, skipLinkChecking=False):
      data=db.isLink(ids) or db.get(ids, existChecked=props, returnRaw=True)
      tArr['branch'][ids]=data
   return tArr

def dumpTree(db, tree, version=1, **kwargs):
   tObjs, tLinks=loadTree(db, tree, onlyPrep=True, version=version)
   tArr={'branch':{}}
   for ids, data in tObjs+tLinks:
      tArr['branch'][ids]=data
   return tArr

def diffDB(db, dumped, branch=None, checkMeta=True, checkProps=True):
   #! нужно добавить сравнение `meta` и `props`
   idsExisted=set()
   if branch:
      idsExisted.add(db._prepIds(branch))
   for ids, (props, branchLen) in db.iterBranch(branch, treeMode=True, safeMode=False, calcProperties=True, skipLinkChecking=False):
      if ids not in dumped['branch']:
         yield ('BRANCH_UNEXPECTED', ids, None)
      else:
         data=db.isLink(ids) or db.get(ids, existChecked=props, returnRaw=True)
         if dumped['branch'][ids]!=data:
            yield ('BRANCH_DATA_MISMATCH', ids, (dumped['branch'][ids], data))
         else:
            idsExisted.add(ids)
   #
   for ids in set(dumped['branch'])-idsExisted:
      yield ('BRANCH_MISSED', ids, None)

# def toTree(db):
#    res={}
#    for ids, (props, branchLen) in db.iterBranch(branch, treeMode=True, safeMode=False, calcProperties=False, skipLinkChecking=False):
#       data=db.get(ids, existChecked=props, returnRaw=True)

def loadTree(db, tree, onlyPrep=False, version=1, **kwargs):
   #! добавить возможность загружать дерево в конкретную ветку
   if version==1:
      return loadTree_v1(db, tree, onlyPrep=onlyPrep, **kwargs)
   else:
      raise ValueError("Unknown method's version: %s"%(version,))

def loadTree_v1(db, tree, onlyPrep=False):
   tObjs=[]
   tLinks=[]
   tQueue=deque(((None, tree),))
   while tQueue:
      parent, tree=tQueue.pop()
      parent=parent or ()
      for action, sub in tree.iteritems():
         if not action: continue
         elif isinstance(action, (str, unicode)):
            ids, data=action, None
         elif isinstance(action, tuple):
            if len(action)==2:
               ids, data=action
            else:
               raise ValueError('Incorrect length for action: %r'%(action,))
         else:
            raise ValueError('Incorrect action: %r'%(action,))
         if not isinstance(ids, (str, unicode)):
            raise ValueError('Incorrect ids for action: %r'%(action,))
         ids=parent+(ids,)
         data={} if data is None else data
         if isinstance(data, tuple):
            tLinks.append((ids, data))
            data=True
         tObjs.append((ids, data))
         #
         if sub and isinstance(sub, dict): tQueue.append((ids, sub))
   tObjs.sort(key=len)
   tLinks.sort(key=len)
   if onlyPrep:
      return tObjs, tLinks
   #
   for ids, data in tObjs:
      db.set(ids, data, strictMode=True, onlyIfExist=False)
   for idsFrom, idsTo in tLinks:
      db.link(idsFrom, idsTo, strictMode=True, onlyIfExist=True)
