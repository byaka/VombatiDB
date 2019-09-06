# -*- coding: utf-8 -*-
__ver_major__ = 0
__ver_minor__ = 3
__ver_patch__ = 0
__ver_sub__ = "dev"
__version__ = "%d.%d.%d%s" % (__ver_major__, __ver_minor__, __ver_patch__, ('-'+__ver_sub__) if __ver_sub__ else '')
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

import signal, atexit
from .utils import *

#? баблинг Props сквозь ссылки (sprout)

class DBBase(object):
   version=__version__

   def __init__(self, workspace, *args, **kwargs):
      self._main_app=sys.modules['__main__']
      self.inited=False
      self.connected=False
      self.settings_frozen=False
      self.workspace=workspace
      self._speedStats=defaultdict(lambda:deque2(maxlen=99999))
      self._speedStatsMax=defaultdict(int)
      self.settings=self._settings=MagicDictCold({})
      self.supports=self._supports=MagicDictCold({})
      self.__propMap={}
      self.__propCompiled={
         'inheritCBMap':{},
         'bubble':set(), 'bubbleCBMap':{},
         'needed':set(),
         'default':{},
         'persistent':set(),
         'defaultMinimalProps':{},
         'mergerBubble':None,
         'mergerInherit':None,
      }
      self.__propCompiled['mergerBubble']=bind(dictMergeEx, {
         'modify':True,
         'recursive':False,
         'cbMap':self.__propCompiled['bubbleCBMap'],
         'cbPassKey':True,
         'cbSkipIfNewKey':False,
         'filterKeys':self.__propCompiled['bubble'],
         'isBlacklist':False,
         'changedCBPassValues':1,
      })
      self.__propCompiled['mergerInherit']=bind(dictMergeEx, {
         'modify':True,
         'recursive':False,
         'cbMap':self.__propCompiled['inheritCBMap'],
         'cbPassKey':True,
         'cbSkipIfNewKey':False,
         'filterKeys':self.__propCompiled['inheritCBMap'],
         'isBlacklist':False,
         'changedCBPassValues':1,
      })
      self.__supportsDefault={
         'meta':True, 'index':True, 'props':True, 'data':False, 'links':True,
         'persistentMeta':False, 'inMemoryMeta':True,
         'persistentData':False, 'inMemoryData':False,
         'persistentProps':False, 'inMemoryProps':True,
         'detailedDiffData': False, 'detailedDiffProps': False,
         'picklingMeta':True, 'picklingData':False, 'picklingProperties':False,
         'prop_link':True, 'prop_backlink':True,
         'generateIdRandom':'Use `None` instead of id, for example `("a1", "b1", None)`.'
      }
      self._idBadPattern=set()
      # self._branchLock=defaultdict(self.workspace.rlock)
      self.___indexNodeClass=dict()
      #
      kwargs2=self._init(*args, **kwargs)
      kwargs2=kwargs2 if isDict(kwargs2) else {}
      self._inited(**kwargs2)

   def _init(self, *args, **kwargs):
      self._idBadPattern.add('\n')
      #
      self._regProp('link', persistent=True)
      self._regProp('backlink')
      #
      for k, v in self.__supportsDefault.iteritems():
         self.supports.setdefault(k, v)
      #
      self.settings.iterBranch_soLong=0.3  # this tries to switch context while exec `iterBranch(safeMode=True)`
      self.settings.iterBranch_sleepTime=0.000001  # not null, becouse this allows to switch to another io-ready greenlet
      self.settings.randomEx_soLong=0.05
      self.settings.randomEx_maxAttempts=9
      self.settings.randomEx_sleepTime=0.001
      self.settings.return_frozen=True
      self.settings.dataMerge_ex=True
      self.settings.dataMerge_deep=False
      self.settings.force_removeChilds=True
      self.settings.force_removeChilds_calcProps=False
      return kwargs

   def _inited(self, **kwargs):
      self.inited=True
      self._compileIdBadPatterns()
      self._initPreExit()
      self._reset()

   def _compileIdBadPatterns(self):
      conds=[]
      codeAfter=[]
      _tab=' '*3
      reIndex=1
      for o in self._idBadPattern:
         if isinstance(o, (str, unicode)):
            if '"' in o:
               o=re.sub(r'(?<!\\)"','\\"', o)
            if '\n' in o:
               o=re.sub(r'(?<!\\)\n','\\\\n', o)
            conds.append((1, 'if "%s" in data: return True'%o))
         elif isinstance(o, (BadPatternStarts, BadPatternEnds)):
            v=o.value
            l=len(v)
            if '"' in v:
               v=re.sub(r'(?<!\\)"','\\"', v)
            if '\n' in v:
               v=re.sub(r'(?<!\\)\n','\\\\n', v)
            if l==1:
               c, s=0, '0' if isinstance(o, BadPatternStarts) else '-1'
            else:
               c, s=3, ':%i'%l if isinstance(o, BadPatternStarts) else '-%i:'%l
            conds.append((c, 'if data[%s]=="%s": return True'%(s, v)))
         elif isinstance(o, (BadPatternREMatch, BadPatternRESearch)):
            n='RUN._re%i'%reIndex
            m='match' if isinstance(o, BadPatternREMatch) else 'search'
            codeAfter.append('%s=re.compile(r"%s", %i).%s'%(n, o.value, o.flags, m))
            conds.append((5, 'if %s(data) is not None: return True'%n))
            reIndex+=1
         else:
            raise ValueError('Incorrect bad-pattern for id: %s'%(o,))
      conds.sort(key=lambda o: o[0])  # sort checkings by complexity
      code=['import re', 'def RUN(data):']
      for o in conds:
         if isinstance(o[1], (tuple, list)):
            s=''.join(_tab+oo for oo in o[1])
            code.append(s)
         else:
            code.append(_tab+o[1])
      code.append(_tab+'return False')
      code+=codeAfter
      code='\n'.join(code)
      code=compile(code, 'idBadPattern', 'exec')
      tEnv={}
      exec code in tEnv
      self._idBadPattern=tEnv['RUN']

   def stopwatch(self, name):
      def tFunc(mytime=timetime(), name=name, self=self):
         val=timetime()-mytime
         self._speedStats[name].append(val)
         if val>self._speedStatsMax[name]:
            self._speedStatsMax[name]=val
      return tFunc

   def _speedStatsAdd(self, name, val):
      self._speedStats[name].append(val)
      if val>self._speedStatsMax[name]:
         self._speedStatsMax[name]=val

   def stats(self, **kwargs):
      res={}
      res['speedstatsTree']=defaultdict(dict)
      res['speedstatsFlat']=defaultdict(lambda: (0,)*5)
      for k, v in self._speedStats.iteritems():
         if '@' in k:
            name, ext=k.split('@', 1)
         else: name, ext=k, ''
         val=(len(v), min(v), arrMedian(v), max(v), self._speedStatsMax[k])
         res['speedstatsTree'][name][ext]=val
         res['speedstatsFlat'][name]=tuple(res['speedstatsFlat'][name][i]+s if i else max(res['speedstatsFlat'][name][i], s) for i,s in enumerate(val))
      return res

   def _getPropMap(self):
      return copy.deepcopy(self.__propMap), copy.deepcopy(self.__propCompiled)

   def _regProp(self, name, default=None, inherit=False, needed=False, bubble=False, persistent=False):
      """
      ...

      :param str name:
      :param any|None default: Дефолтное значение для новых или пустых `props`.
      :param bool|func|and+|+and|and|or+|+or|or inherit: Задает правило наследования значения от родителей к детям.
      :param bool needed: Является ли данная `prop` обязательной при создании обьекта. Если задан также `default`, будет создано автоматически если не указано явно.
      :param bool|func bubble: Задает правило всплытия значения от детей к родителям.
      :param bool persistent: Указывает, что данная `prop` должна сохраняться "на диск". Данная возможность реализуется не ядром, а бекендом хранения.
      """
      if self.inited:
         raise RuntimeError('Registering of property not allowed, when DB already inited')
      assert isStr(name), 'Name must be string'
      if name in self.__propMap:
         raise ValueError('Property with this name already registered')
      o=self.__propMap[name]={'name':name, 'default':default, 'inherit':inherit, 'needed':needed, 'bubble':bubble, 'persistent':persistent}
      if o['default'] is not None:
         self.__propCompiled['default'][name]=o['default']
      if o['inherit']:
         self.__propCompiled['inheritCBMap'][name]=self._convPropCB_inherit(o)
      if o['bubble']:
         self.__propCompiled['bubble'].add(name)
         if isFunction(o['bubble']):
            self.__propCompiled['bubbleCBMap'][name]=o['bubble']
      if o['needed']:
         self.__propCompiled['needed'].add(name)
      if o['persistent']:
         self.__propCompiled['persistent'].add(name)
      # compiling defaultMinimalProps
      self.__propCompiled['defaultMinimalProps']={}
      for k in set(itertools.chain(self.__propCompiled['needed'], self.__propCompiled['default'])):
         if k in self.__propCompiled['needed'] and k not in self.__propCompiled['default']: break
         self.__propCompiled['defaultMinimalProps'][k]=self.__propCompiled['default'][k]
      else:
         self.__propCompiled['defaultMinimalProps']=None

   def _convPropCB_inherit(self, o):
      f=o['inherit']
      if f is True:
         o['inheritCB']=lambda k, vSelf, vParrent, ids: vParrent
      elif f=='and' or f=='and+':
         o['inheritCB']=lambda k, vSelf, vParrent, ids: vParrent and vSelf
      elif f=='+and':
         o['inheritCB']=lambda k, vSelf, vParrent, ids: vSelf and vParrent
      elif f=='or' or f=='or+':
         o['inheritCB']=lambda k, vSelf, vParrent, ids: vParrent or vSelf
      elif f=='+or':
         o['inheritCB']=lambda k, vSelf, vParrent, ids: vSelf or vParrent
      elif isFunction(f): o['inheritCB']=f
      else:
         raise ValueError('Unsupported value for inherit-callback of Property "%s": %r'%(o['name'], f))
      return o['inheritCB']

   @classmethod
   def _prepIds(cls, ids):
      ids=ids if isinstance(ids, tuple) else (tuple(ids) if isinstance(ids, list) else (ids,))
      return ids

   def checkIds(self, ids, props=None, calcProperties=False):
      stopwatch=self.stopwatch('checkIds%s@DBBase'%('-calcProps' if calcProperties else ''))
      ids=self._prepIds(ids)
      if props is None:
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
      else: isExist=None
      stopwatch()
      return ids, isExist, props

   def isExist(self, ids, andLink=False):
      try:
         ids, isExist, props=self.checkIds(ids, calcProperties=False)
         if andLink:
            return (isExist, self.isLink(props, calcProperties=False))
         else:
            return isExist
      except (BadLinkError, StrictModeError):
         return (False, None) if andLink else False

   def resolveLink(self, ids, linkChain=False, idsPrepared=False, linkChainIsFunc=False):
      stopwatch=self.stopwatch('resolveLink@DBBase')
      if not idsPrepared:
         ids=self._prepIds(ids)
      if linkChain and linkChainIsFunc is True: _linkChain=linkChain
      elif linkChain is False or linkChain is None or not isinstance(linkChain, list): linkChain=False
      else:
         _linkChain=linkChain.append
      tQueue=deque((ids, ))
      _tQueueAppend=tQueue.append
      _tQueuePop=tQueue.popleft
      while tQueue:
         ids=_tQueuePop()
         branch=self.__index
         props=None
         for id in ids:
            if id not in branch:
               stopwatch()
               raise BadLinkError(ids)
            props, branch=branch[id]
            if 'link' in props and props['link']:
               _tQueueAppend(props['link'])
         if linkChain is not False:
            _linkChain((ids, props))
      stopwatch()
      return ids

   def isLink(self, idsOrProps, calcProperties=False):
      stopwatch=self.stopwatch('isLink%s@DBBase'%('-calcProps' if calcProperties else ''))
      ids=props=idsOrProps
      if not isinstance(ids, dict):
         ids=self._prepIds(ids)
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
         if not isExist:
            stopwatch()
            return (None, None) if calcProperties else None
      res=props['link'] if 'link' in props else False
      stopwatch()
      return (res, props) if calcProperties else res

   def _initPreExit(self):
      #! перенести функцию в funxtionsex
      atexit.register(self.close)
      def tFunc(sigId, stack, self_close=self.close, old=None):
         self_close()
         if old is not None:
            old(sigId, stack)
         sys.exit(0)
      for s in (signal.SIGTERM, signal.SIGINT):
         old=signal.getsignal(s)
         if not isFunction(old): old=None
         signal.signal(s, bind(tFunc, {'old':old}))

   def isGoodId(self, ids, needException=False):
      ids=(ids,) if isinstance(ids, (str, unicode)) else ids
      for id in ids:
         r=not self._idBadPattern(id)
         if r: continue
         elif needException:
            raise BadIdError(id)
         else: return False
      return True

   def _initMeta(self):
      self.__meta={}
      self._loadedMeta(self.__meta)

   def _initIndex(self):
      self.__index=self.___indexNodeClass()
      self._loadedIndex(self.__index)

   def _loadedMeta(self, data):
      pass

   def _loadedIndex(self, data):
      pass

   def _connect(self, **kwargs):
      pass

   def connect(self, andReset=False, **kwargs):
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=defaultdict(bool, **self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      if andReset:
         self._reset()
      self._connect(andReset=andReset, **kwargs)
      self.connected=True

   def _reset(self):
      self._initMeta()
      self._initIndex()

   def _markInIndex(self, ids, strictMode=True, createNotExisted=False, skipBacklinking=False, _changes=None, **propsUpdate):
      stopwatch=self.stopwatch('_markInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      stopwatch1=self.stopwatch('_markInIndex.prepProps@DBBase')
      for k in propRules['needed']:
         if k in propsUpdate: continue
         if k not in propRules['default']:
            stopwatch()
            raise ValueError('Property "%s" missed and no default value: %s'%(k, ids))
         propsUpdate[k]=propRules['default'][k]
      stopwatch1()
      #
      if _changes is None or _changes is False or not isinstance(_changes, dict): _changes=None
      branch=self.__index
      for i, id in enumerate(ids):
         isExist=id in branch
         if isExist:
            propsCurrent=branch[id][0]
         ids2=ids[:i+1]
         if i==iLast:
            if isExist:
               if propsUpdate:
                  _oldLink=branch[id][0]['link'] if 'link' in propsUpdate and 'link' in branch[id][0] else None
                  stopwatch1=self.stopwatch('_markInIndex.mergeProps@DBBase')
                  if _changes is None:
                     branch[id][0].update(propsUpdate)
                  else:
                     tDiff={}
                     dictMergeEx(branch[id][0], propsUpdate, modify=True, recursive=False, changedCB=tDiff, changedCBPassValues=1)
                     if tDiff:
                        _changes[ids2]=tDiff
                  stopwatch1()
                  # detect changes of link
                  if 'link' in propsUpdate and branch[id][0]['link']!=_oldLink:
                     try: self._linkModified(ids, branch[id][0], branch[id][1], True, _oldLink, skipBacklinking=skipBacklinking)
                     except StrictModeError:
                        if strictMode: raise
            else:
               if not propsUpdate: branch[id]=({}, {})
               else:
                  tCopy=propsUpdate.copy()
                  branch[id]=(tCopy, {})
                  if _changes is not None: _changes[ids2]=tCopy
                  # detect changes of link
                  if 'link' in propsUpdate:
                     try: self._linkModified(ids, branch[id][0], branch[id][1], False, None, skipBacklinking=skipBacklinking)
                     except StrictModeError:
                        if strictMode: raise
            stopwatch()
            return isExist
         elif not isExist:
            if strictMode or not createNotExisted:
               stopwatch()
               raise ParentNotExistError('"%s" for %s'%(ids2, ids))
            elif propRules['defaultMinimalProps'] is None:
               stopwatch()
               raise ValueError("Parent '%s' not exists and some property's default vals missed: %s"%(ids2, ids))
            self.workspace.log(2, 'Parent "%s" not exists, creating it with default properties: %s'%(ids2, ids))
            propsCurrent=propRules['defaultMinimalProps'].copy()
            branch[id]=(propsCurrent, {})
         # and now bubbling props if needed (also for missed parents in non-strict mode)
         if propMerger and propsUpdate:
            stopwatch1=self.stopwatch('_markInIndex.bubblingProps@DBBase')
            tDiff=None if _changes is None else {}
            propMerger(propsCurrent, propsUpdate, cbArgs=(ids2,), changedCB=tDiff)
            if tDiff:
               _changes[ids2]=tDiff
            stopwatch1()
         branch=branch[id][1]
      stopwatch()

   def _unmarkInIndex(self, ids, skipBacklinking=False, _changes=None, propsOld=None, **propsUpdate):
      stopwatch=self.stopwatch('_unmarkInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      if _changes is None or _changes is False or not isinstance(_changes, dict): _changes=None
      branch=self.__index
      for i, id in enumerate(ids):
         if i==iLast: break
         if id not in branch:
            branch=None
            break
         # and now bubbling props if needed (also for missed parents in non-srict mode)
         if propMerger and propsUpdate:
            stopwatch1=self.stopwatch('_unmarkInIndex.bubblingProps@DBBase')
            ids2=ids[:i+1]
            #? плохо, что в данном случае баблинг происходит даже если удаляемого обьекта несуществует, ибо проверка на его наличие идет позже
            tDiff=None if _changes is None else {}
            propMerger(branch[id][0], propsUpdate, cbArgs=(ids2,), changedCB=tDiff)
            if tDiff:
               _changes[ids2]=tDiff
            stopwatch1()
         branch=branch[id][1]
      if branch is None or ids[-1] not in branch:
         if propsOld is not None and 'link' in propsOld and propsOld['link']:
            self._linkModified(ids, None, None, True, propsOld['link'], skipBacklinking=skipBacklinking)
         stopwatch()
         return False
      _oldLink=branch[ids[-1]][0].get('link', None)
      del branch[ids[-1]]
      if _oldLink:
         self._linkModified(ids, None, None, True, _oldLink, skipBacklinking=skipBacklinking)
      stopwatch()
      return True

   def _linkModified(self, ids, props, branch, wasExisted, oldLink, skipBacklinking=False):
      _status=None
      if skipBacklinking: return _status
      elif branch is None or props['link'] is None:
         if not oldLink: return _status
         _del, _add=oldLink, None  # ссылка была удалена
         _status='REMOVED'
      elif not oldLink:
         _del, _add=None, props['link']  # ссылка была создана
         _status='CREATED'
      else:
         if oldLink==props['link']: return _status  # защита на всякий случай
         _del, _add=oldLink, props['link']  # ссылка была изменена
         _status='EDITED'
      #
      if _del is not None:
         stopwatch=self.stopwatch('_linkModified.backlinkDel@DBBase')
         isExist, o, _=self._findInIndex(_del, strictMode=False, calcProperties=False)
         if isExist is True and 'backlink' in o and o['backlink'] and ids in o['backlink']:
            if len(o['backlink'])==1: backlinkNew=set()
            else:
               backlinkNew=o['backlink'].copy()
               backlinkNew.remove(ids)
            self._markInIndex(_del, backlink=backlinkNew)
         stopwatch()
      if _add is not None:
         stopwatch=self.stopwatch('_linkModified.backlinkAdd@DBBase')
         isExist, o, _=self._findInIndex(_add, strictMode=True, calcProperties=False)
         if not isExist:
            raise NotExistError(_add)
         else:
            if 'backlink' not in o: backlinkNew=set((ids,))
            elif ids not in o['backlink']:
               backlinkNew=o['backlink'].copy()
               backlinkNew.add(ids)
            else:
               stopwatch()
               return _status
            self._markInIndex(_add, backlink=backlinkNew)
         stopwatch()
      return _status

   def countBacklinks(self, ids, props=None, strictMode=False, recursive=False, allowContextSwitch=False):
      mytime=timetime()
      ids=self._prepIds(ids)
      if props is None:
         badLinkChain=[]
         try:
            isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=False, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            return 0
         except ParentNotExistError:
            isExist=False
         if not isExist:
            if strictMode:
               raise NotExistError(ids)
            else:
               return 0
      if not props or 'backlink' not in props or not props['backlink']:
         return 0
      backlink=props['backlink']
      # fast-case
      if not recursive:
         return len(backlink)
      # iterating
      _soLong=self._settings['iterBranch_soLong'] if allowContextSwitch else False
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.sleep
      _timetime=timetime
      queue=deque((backlink,))
      _queueAppend=queue.append
      _queuePop=queue.pop
      res=0
      while queue:
         backlink=_queuePop()
         for ids in backlink:
            if _soLong and _timetime()-mytime>=_soLong:
               _sleep(_sleepTime)
               mytime=_timetime()
            isExist, propsCurrent, branchCurrent=self._findInIndex(ids, strictMode=True, calcProperties=False)
            if not isExist:
               if strictMode:
                  raise NotExistError(ids)
               else:
                  continue
            res+=1
            if not recursive: continue
            if not propsCurrent or 'backlink' not in propsCurrent or not propsCurrent['backlink']: continue
            backlinkCurrent=propsCurrent['backlink']
            _queueAppend(backlinkCurrent)
      return res

   def getBacklinks(self, ids, props=None, strictMode=True, safeMode=True):
      ids=self._prepIds(ids)
      if props is None:
         badLinkChain=[]
         try:
            isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=True, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            raise StopIteration
         except ParentNotExistError:
            isExist=False
         if not isExist:
            if strictMode:
               raise NotExistError(ids)
            else:
               return set()
      if not props or 'backlink' not in props or not props['backlink']:
         return set()
      return props['backlink'].copy() if safeMode else props['backlink']

   def iterBacklinks(self, ids, props=None, recursive=False, treeMode=True, safeMode=True, calcProperties=True, strictMode=True, allowContextSwitch=True):
      mytime=timetime()
      ids=self._prepIds(ids)
      if props is None:
         badLinkChain=[]
         try:
            isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            raise StopIteration
         except ParentNotExistError:
            isExist=False
         if not isExist:
            if strictMode:
               raise NotExistError(ids)
            else:
               raise StopIteration
      if not props or 'backlink' not in props or not props['backlink']:
         raise StopIteration
      _soLong=self._settings['iterBranch_soLong'] if allowContextSwitch else False
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.sleep
      _timetime=timetime
      _len=len
      _iter=iter
      backlink=props['backlink']
      queue=deque(((_iter(backlink.copy() if safeMode else backlink), backlink),))
      _queueAppend=queue.append
      _queuePop=queue.pop
      while queue:
         iterBacklinks, backlink=_queuePop()
         for ids in iterBacklinks:
            if _soLong and _timetime()-mytime>=_soLong:
               _sleep(_sleepTime)
               mytime=_timetime()
            if safeMode and ids not in backlink: continue
            isExist, propsCurrent, branchCurrent=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
            if not isExist:
               if strictMode:
                  raise NotExistError(ids)
               else:
                  continue
            extCmd=yield ids, (propsCurrent, _len(branchCurrent))
            if extCmd is not None:
               yield  # this allows to use our generator inside `for .. in ..` without skipping on `send`
               if extCmd is False: continue
            if not recursive: continue
            if not propsCurrent or 'backlink' not in propsCurrent or not propsCurrent['backlink']: continue
            backlinkCurrent=propsCurrent['backlink']
            iterBacklinksCurrent=_iter(backlinkCurrent.copy() if safeMode else backlinkCurrent)
            if treeMode:
               _queueAppend((iterBacklinks, backlink))
               _queueAppend((iterBacklinksCurrent, backlinkCurrent))
               break
            else:
               _queueAppend((iterBacklinksCurrent, backlinkCurrent))

   def countBranch(self, ids=None, strictMode=False, offsetLast=False, recursive=False, allowContextSwitch=False, skipLinkChecking=False):
      mytime=timetime()
      if ids is not None:
         ids=self._prepIds(ids)
         # searching enter-point and calc props if needed
         badLinkChain=[]
         try:
            isExist, _, branchCurrent=self._findInIndex(ids, strictMode=True, calcProperties=False, offsetLast=offsetLast, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            return 0
         except ParentNotExistError:
            isExist=False
         if not isExist:
            if strictMode: raise NotExistError(ids)
            else: return 0
         if offsetLast: ids=ids[:-1]
      else:
         ids, branchCurrent=(), self.__index
      # fast-case
      if not branchCurrent: return 0
      elif not recursive and skipLinkChecking: return len(branchCurrent)
      # iterating
      _soLong=self._settings['iterBranch_soLong'] if allowContextSwitch else False
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.sleep
      _timetime=timetime
      _resolveLink=self.resolveLink
      queue=deque(((ids, branchCurrent),))
      _queueAppend=queue.append
      _queuePop=queue.pop
      res=0
      while queue:
         ids, branchParrent=_queuePop()
         for id in branchParrent:
            if _soLong and _timetime()-mytime>=_soLong:
               _sleep(_sleepTime)
               mytime=_timetime()
            propsCurrent, branchCurrent=branchParrent[id]
            # validating link's target
            if not skipLinkChecking and 'link' in propsCurrent and propsCurrent['link']:
               _resolveLink(propsCurrent['link'], idsPrepared=True)
            ids2=ids+(id,)
            res+=1
            if not recursive: continue
            if not branchCurrent: continue
            _queueAppend((ids2, branchCurrent))
      return res

   def getBranch(self, ids=None, strictMode=True, safeMode=True, recursive=True, offsetLast=False):
      if ids is not None:
         ids=self._prepIds(ids)
         # searching enter-point and calc props if needed
         badLinkChain=[]
         try:
            isExist, _, branch=self._findInIndex(ids, strictMode=True, calcProperties=True, offsetLast=offsetLast, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            if strictMode: raise NotExistError(ids)
            else: return ()
         except ParentNotExistError:
            if strictMode: raise NotExistError(ids)
            else: return ()
         if not isExist:
            if strictMode: raise NotExistError(ids)
            else: return ()
      else:
         branch=self.__index
      return branch.keys() if safeMode else branch

   def iterBranch(self, ids=None, strictMode=True, recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True, skipLinkChecking=False, allowContextSwitch=True, returnParent=False):
      #! добавить возможность передавать сюда `props`
      mytime=timetime()
      _soLong=self._settings['iterBranch_soLong'] if allowContextSwitch else False
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.sleep
      _timetime=timetime
      _len=len
      _iter=iter
      _resolveLink=self.resolveLink
      propRules=self.__propCompiled
      propMerger=propRules['mergerInherit'] if calcProperties and propRules['inheritCBMap'] else False
      if ids is not None:
         ids=self._prepIds(ids)
         # searching enter-point and calc props if needed
         badLinkChain=[]
         try:
            isExist, propsPre, branchCurrent=self._findInIndex(ids, strictMode=True, calcProperties=propMerger, offsetLast=offsetLast, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            raise StopIteration
         except ParentNotExistError:
            isExist=False
         if not isExist:
            if strictMode: raise NotExistError(ids)
            else: raise StopIteration
         if offsetLast: ids=ids[:-1]
      else:
         if propMerger and propRules['defaultMinimalProps'] is not None:
            propsPre=propRules['defaultMinimalProps'].copy()
         else: propsPre={}
         ids, branchCurrent=(), self.__index
      # iterating
      if not branchCurrent:
         raise StopIteration
      queue=deque(((ids, _iter(branchCurrent.keys()) if safeMode else branchCurrent.iterkeys(), branchCurrent, propsPre),))
      _queueAppend=queue.append
      _queuePop=queue.pop
      #? если в процессе итерации был изменен `props`, мы об этом не узнаем и для дочерних элементов продолжаем использовать исходный `props`
      while queue:
         ids, iterParrent, branchParrent, propsParrent=_queuePop()
         for id in iterParrent:
            if _soLong and _timetime()-mytime>=_soLong:
               _sleep(_sleepTime)
               mytime=_timetime()
            if id not in branchParrent: continue
            propsCurrent, branchCurrent=branchParrent[id]
            # validating link's target
            if not skipLinkChecking and 'link' in propsCurrent and propsCurrent['link']:
               _resolveLink(propsCurrent['link'], idsPrepared=True)
            ids2=ids+(id,)
            # and now we inherit props if needed
            if propMerger and propsParrent:
               propsCurrent=propsCurrent.copy() if propsCurrent else {}
               propMerger(propsCurrent, propsParrent, cbArgs=(ids2,))
            else:
               propsCurrent=(propsCurrent.copy() if propsCurrent else {}) if propMerger else propsCurrent
            if returnParent is True:
               extCmd=yield (ids, (propsParrent, _len(branchParrent))), (ids2, (propsCurrent, _len(branchCurrent)))
            else:
               extCmd=yield ids2, (propsCurrent, _len(branchCurrent))
            if extCmd is not None:
               yield  # this allows to use our generator inside `for .. in ..` without skipping on `send`
               if extCmd is False: continue
            if not recursive: continue
            if not branchCurrent: continue
            iterCurrent=_iter(branchCurrent.keys()) if safeMode else branchCurrent.iterkeys()
            if treeMode:
               _queueAppend((ids, iterParrent, branchParrent, propsParrent))
               _queueAppend((ids2, iterCurrent, branchCurrent, propsCurrent))
               break
            else:
               _queueAppend((ids2, iterCurrent, branchCurrent, propsCurrent))

   def _findInIndex(self, ids, strictMode=False, calcProperties=True, offsetLast=False, linkChain=False, skipLinkChecking=False, parentsChain=False):
      # поиск обьекта в индексе и проверка, существует ли вся его иерархия
      _stopwatch=self.stopwatch
      stopwatch=_stopwatch('_findInIndex%s%s@DBBase'%('-calcProps' if calcProperties else '', '-noLinkCheck' if skipLinkChecking else ''))
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerInherit'] if calcProperties and propRules['inheritCBMap'] else False
      if propMerger and propRules['defaultMinimalProps'] is not None:
         tArr1=propRules['defaultMinimalProps'].copy()
      else: tArr1={}
      res=[None, tArr1, self.__index]
      if propMerger:
         propsQueue=[None]*iLast  #! оптимизировать
      if linkChain is False or linkChain is None or not isinstance(linkChain, list):
         linkChain, _linkChain=False, None
      else:
         linkChainI=len(linkChain)
         _linkChain=linkChain.append
         linkChain.append((ids, None))  # we change it later, when we will have props
      if parentsChain is False or parentsChain is None or not isinstance(parentsChain, list):
         parentsChain, _parentsChain=False, None
      else:
         _parentsChain=parentsChain.append
      for i, id in enumerate(ids):
         if offsetLast and i==iLast: break
         if id is None and i==iLast:
            # айди не указан, используется при автоматической генерации айди
            res[0], res[1], res[2]=None, {}, None
            break
         if id not in res[2]:
            if i==iLast: res[0]=False
            else:
               if strictMode:
                  stopwatch()
                  raise ParentNotExistError('"%s" for %s'%(id, ids))
               res[0]=None
            res[1], res[2]={}, None
            break
         res[0]=True
         propsCurrent, res[2]=res[2][id]
         # validating link's target
         if not skipLinkChecking and 'link' in propsCurrent and propsCurrent['link']:
            self.resolveLink(propsCurrent['link'], linkChain=_linkChain, idsPrepared=True, linkChainIsFunc=True)
         # and now we inherit props if needed
         if propMerger:
            propsQueue[i]=res[1]
         res[1]=propsCurrent
         if parentsChain is not False and i!=iLast:
            #! не наследуются props для родителей
            _parentsChain((res[1], res[2]))
         #? нужно протестировать новый механизм наследования, смотри issue#32
         # if propMerger and res[1]:
         #    propsParrent, res[1]=res[1], propsCurrent.copy() if propsCurrent else {}
         #    propMerger(res[1], propsParrent, cbArgs=(ids[:i+1],))
         # else:
         #    res[1]=(propsCurrent.copy() if propsCurrent else {}) if propMerger else propsCurrent
      # and now we inherit props if needed
      if propMerger:
         res[1]=res[1].copy() if res[1] else {}
         propMerger(res[1], propsQueue, cbArgs=(ids,))
      if parentsChain is not False:
         _parentsChain((res[1], res[2]))
      #
      if linkChain is not False:
         linkChain[linkChainI]=(ids, res[1])
      stopwatch()
      return res

   def _parseMeta(self, data):
      data=pickle.loads(data)
      if not isinstance(data, dict):
         raise TypeError('Meta must be a dict')
      self.__meta=dictMergeEx(data, self.__meta, modify=True, recursive=True)
      self._loadedMeta(self.__meta)

   def _dumpMeta(self):
      return pickle.dumps(self.__meta, pickle.HIGHEST_PROTOCOL)

   def _saveMetaToStore(self, **kwargs):
      pass

   def _dataMerge(self, o1, o2, changed=None, changedType='key', **kwargs):
      stopwatch=self.stopwatch('_dataMerge@DBBase')
      if self._settings['dataMerge_ex']:
         #! перевети на dictMergeEx
         dictMerge(o1, o2, changed=changed, changedType=changedType, modify=True, recursive=self._settings['dataMerge_deep'])
         stopwatch()
         return True
      else:
         o1.update(o2)
         stopwatch()

   def randomExSoLongCB(self, mult, vals, pref, suf, i):
      if i>self._settings['randomEx_maxAttempts']:
         self.workspace.log(0, 'randomEx generating value so long for (%s, %s, %s), %s attempts, aborted'%(pref, mult, suf, i))
         raise OverflowError('randomeEx generating so long (%s attempts)'%i)
      self.workspace.log(2, 'randomEx generating value so long for (%s, %s, %s), attempt %s'%(pref, mult, suf, i))
      self.workspace.sleep(self._settings['randomEx_sleepTime'])
      return mult

   def _generateId(self, ids, offsetLast=True, **kwargs):
      # создает новый айди для новой сущности
      stopwatch=self.stopwatch('_generateId@DBBase')
      ids=ids[:-1] if offsetLast else ids
      tArr=self._findInIndex(ids, strictMode=True, calcProperties=False, offsetLast=False)
      newId=randomEx(vals=tArr, soLong=self._settings['randomEx_soLong'], cbSoLong=self.randomExSoLongCB)
      ids+=(newId,)
      stopwatch()
      return ids

   def link(self, idsTo, idsFrom, existChecked=None, onlyIfExist=None, strictMode=False):
      idsTo=self._prepIds(idsTo)
      idsFrom=self._prepIds(idsFrom)
      return self.set(idsTo, True, existChecked=existChecked, propsUpdate={'link':idsFrom}, onlyIfExist=onlyIfExist, strictMode=strictMode)

   def remove(self, ids, existChecked=None, strictMode=False):
      return self.set(ids, None, existChecked=existChecked, onlyIfExist=True, strictMode=strictMode)

   def move(self, idsFrom, idsTo, onlyIfExist=None, strictMode=True, fixLinks=True, recursive=True):
      stopwatch=self.stopwatch('move@DBBase')
      idsFrom=self._prepIds(idsFrom)
      idsTo=self._prepIds(idsTo)
      idsFromMain=idsFrom
      #? в теории можно перевести весь этот код на `iterBranch`, тем самым стандартизировав его. однако в таком случае придется выполнять дополнительные манипуляции для конструирования нового `idsTo` для каждого ребенка, в то время как в текущем коде новый `idsTo` выводится сам последовательно.
      #! не реализована работа с props - подробности в *issue#50*
      tQueue=deque((((idsFrom, NULL, NULL), idsTo),))
      while tQueue:
         (idsFrom, props, branch), idsTo=tQueue.pop()
         if self.isExist(idsTo):
            #! onlyIfExist
            stopwatch()
            raise AlreadyExistError(idsTo)
         if props is NULL:
            try:
               badLinkChain=[]
               isExist, props, branch=self._findInIndex(idsFrom, strictMode=True, calcProperties=True, linkChain=badLinkChain)
            except BadLinkError:
               # удаляем плохой линк
               for _ids, _props in reversed(badLinkChain):
                  self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
               stopwatch()
               if strictMode: raise NotExistError(idsFrom)
               else: return None
            except ParentNotExistError:
               isExist=False
            if not isExist:
               if strictMode:
                  stopwatch()
                  raise NotExistError(idsFrom)
               else:
                  stopwatch()
                  return None
         else:
            isExist=True
         # for link we dont need data
         if self.isLink(props, calcProperties=False):
            #! onlyIfExist
            r=self.link(idsTo, props['link'], onlyIfExist=False, strictMode=strictMode)
            assert r is not None
         else:
            # get old data
            data=self.get(idsFrom, existChecked=props, returnRaw=True, strictMode=strictMode)
            # moving obj
            #! onlyIfExist
            r=self.set(idsTo, data, allowMerge=False, onlyIfExist=False, strictMode=strictMode)
            assert r is not None
         # fixing links
         if fixLinks and 'backlink' in props:
            for idsLinked in props['backlink']:
               self.link(idsLinked, idsTo, onlyIfExist=True, strictMode=strictMode)
         # moving childs
         if recursive and branch:
            for _id in branch:
               _props, _branch=branch[_id]
               _idsFrom=idsFrom+(_id,)
               _idsTo=idsTo+(_id,)
               tQueue.append(((_idsFrom, _props, _branch), _idsTo))
      r=self.remove(idsFromMain, strictMode=strictMode)
      assert r is not None
      stopwatch()
      return idsTo

   def _validateOnSet(self, ids, data, isExist=None, allowMerge=None, **kwargs):
      # хук, позволяющий проверить или модифицировать данные (и Props) перед их добавлением
      return isExist, data, allowMerge

   def set(self, ids, data, allowMerge=True, existChecked=None, propsUpdate=None, allowForceRemoveChilds=True, onlyIfExist=None, strictMode=False, **kwargs):
      # если вызывающий хочет пропустить проверку `self._findInIndex()`, нужно передать в `existChecked`, высчитанные `props` (в таком случае будет считаться, что обьект существует) илиже `(isExist, props)` если нужно указать непосредственный статус `isExist`
      stopwatch=self.stopwatch('set@DBBase')
      if not propsUpdate: propsUpdate={}
      elif not isinstance(propsUpdate, dict):
         raise ValueError('Incorrect type, `propsUpdate` must be a dict')
      if data is not None and data is not True and data is not False and not isinstance(data, dict):
         raise ValueError('Incorrect data format')
      ids=self._prepIds(ids)
      if existChecked is None:
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=True, skipLinkChecking=True)
      else:
         isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
      if not ids[-1]:
         # идентификатор не указан, требуется сгенерировать его
         ids=self._generateId(ids, props=props)
      if not isExist and self._idBadPattern(ids[-1]) is True:
         raise BadIdError(ids[-1])
      if onlyIfExist is not None and isExist!=onlyIfExist:
         if strictMode:
            raise ExistStatusMismatchError('expected "isExist=%s" for %s'%(onlyIfExist, ids))
         return ids
      if data is not True and 'link' in props and props['link']: propsUpdate['link']=None
      # если создается ссылка, проверяем существование обьекта
      if 'link' in propsUpdate:
         stopwatch1=self.stopwatch('set.fixLink@DBBase')
         if 'link' in props and props['link'] and props['link']==propsUpdate['link']:
            del propsUpdate['link']
         elif propsUpdate['link'] is not None:
            try:
               badLinkChain=[]
               self.resolveLink(propsUpdate['link'], linkChain=badLinkChain.append, idsPrepared=True, linkChainIsFunc=True)
            except BadLinkError, e:
               # удаляем плохой линк
               _set=self.set
               for ids, props in reversed(badLinkChain):
                  _set(ids, None, existChecked=props, allowForceRemoveChilds=True)
               stopwatch1()
               stopwatch()
               raise e
         stopwatch1()
      #
      isExist, data, allowMerge=self._validateOnSet(ids, data, isExist=isExist, props=props, allowMerge=allowMerge, propsUpdate=propsUpdate)
      if data is None and not isExist:
         stopwatch()
         return ids
      else:
         if data is False: data=True  #! ВРЕМЕННЫЙ ФИКС
         tArr=((ids, (isExist, data, allowMerge, props, propsUpdate)),)
         self._setData(tArr, **kwargs)
      if data is None:
         if self._settings['force_removeChilds'] and allowForceRemoveChilds:
            # принудительно удаляем детей, чтобы избежать конфликта при повторном использовании техже имен
            stopwatch1=self.stopwatch('set.removeChilds@DBBase')
            for _ids, (_props, _) in self.iterBranch(ids=ids, recursive=True, treeMode=True, safeMode=True, skipLinkChecking=True, calcProperties=self._settings['force_removeChilds_calcProps'], allowContextSwitch=False):  # noqa: E501
               tArr=((_ids, (True, None, False, _props, {})),)
               self._setData(tArr, **kwargs)
               self._unmarkInIndex(_ids, propsOld=_props)
               #~ такой подход к удалению является серьезной оптимизацией, однако эту особенность нужно иметь ввиду при разработке расширений
            stopwatch1()
         self._unmarkInIndex(ids, **propsUpdate)
      elif not isExist or propsUpdate:
         self._markInIndex(ids, **propsUpdate)
      stopwatch()
      return ids

   def _setData(self, items, **kwargs):
      pass

   def get(self, ids, existChecked=None, returnRaw=False, strictMode=False, **kwargs):
      stopwatch=self.stopwatch('get@DBBase')
      ids=self._prepIds(ids)
      if ids[-1] is None:
         # this case allowed by `_findInIndex()` so we need to check it manually
         raise ValueError('Incorrect IDS: %s'%(ids,))
      # если вызывающий хочет пропустить проверку `self._findInIndex()`, нужно передать в `existChecked`, высчитанные `properties` (в таком случае будет считаться, что обьект существует) илиже `(isExist, properties)` если нужно указать непосредственный статус `isExist`
      try:
         badLinkChain=[]
         if existChecked is None:
            isExist, props, _=self._findInIndex(ids, strictMode=strictMode, calcProperties=True, linkChain=badLinkChain)
            if not isExist:
               stopwatch()
               if strictMode:
                  raise NotExistError(ids)
               return None
            ids=badLinkChain[-1][0]  # getting target, if this was link
         else:
            isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
            ids=self.resolveLink(ids, linkChain=badLinkChain.append, idsPrepared=True, linkChainIsFunc=True)
      except BadLinkError, e:
         if strictMode:
            stopwatch()
            raise StrictModeError(e)
         # удаляем плохой линк
         for _ids, _props in reversed(badLinkChain):
            self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
         stopwatch()
         return None
      res=self._getData(ids, props, **kwargs)
      if res is not True and not returnRaw:
         if self._settings['return_frozen']:
            res=MagicDictCold(res)
            res._MagicDictCold__freeze()
         else:
            res=MagicDict(res)
      stopwatch()
      return res

   def _getData(self, ids, props, **kwargs):
      pass

   def truncate(self, **kwargs):
      self._reset()

   def close(self, *args, **kwargs):
      if getattr(self, '_destroyed', False): return
      self.workspace.log(4, 'Closing db')
      self._close(*args, **kwargs)
      #
      def tFunc(*args, **kwargs):
         self.workspace.log(2, 'DB already closed')
      for k in dir(self):
         if k.startswith('__') and k.endswith('__'): continue
         if k=='close' or k=='workspace': continue
         o=getattr(self, k)
         if o is None: continue
         if isinstance(o, (types.FunctionType, types.MethodType)): o=tFunc
         else: o=None
         setattr(self, k, o)
      self._destroyed=True

   def _close(self, *args, **kwargs):
      pass

   def __enter__(self):
      return self

   def __exit__(self, *err):
      self.close()
