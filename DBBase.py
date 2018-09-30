# -*- coding: utf-8 -*-
import signal, atexit
from utils import *

__ver_major__ = 0
__ver_minor__ = 1
__ver_patch__ = 0
__ver_sub__ = "dev"
__version__ = "%d.%d.%d" % (__ver_major__, __ver_minor__, __ver_patch__)
"""
:authors: John Byaka
:copyright: Copyright 2018, Buber
:license: Apache License 2.0

:license:

   Copyright 2018 Buber

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

#? баблинг Props сквозь ссылки (sprout)

class DBBase(object):
   def __init__(self, workspace, *args, **kwargs):
      self.version=__version__
      self._main_app=sys.modules['__main__']
      self.inited=False
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
         'meta':True, 'index':True, 'properties':True, 'links':True,
         'readableData':False, 'writableData':False,
         'persistentIndex':False, 'inMemoryIndex':True,
         'persistentMeta':False, 'inMemoryMeta':True,
         'persistentData':False, 'inMemoryData':False,
         'detailedDiffData': False,
         'picklingMeta':True, 'picklingData':False, 'picklingProperties':False,
         'persistentProperties':False, 'inMemoryProperties':True,
         'prop_link':True, 'prop_backlink':True,
      }
      self._idBadPattern=set()
      # self._branchLock=defaultdict(RLock)
      #
      kwargs2=self._init(*args, **kwargs)
      kwargs2=kwargs2 if isDict(kwargs2) else {}
      self._inited(**kwargs2)

   def _init(self, *args, **kwargs):
      self._idBadPattern.add('\n')
      #
      self._regProp('link', persistent=True)
      self._regProp('backlink', persistent=True)
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
      mytime=timetime()
      #? проверить, что быстрее - создание этой функции в рантайме или использование `bind()`
      def tFunc(mytime=mytime, name=name, self=self):
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

   def getProps(self):
      return copy.deepcopy(self.__propMap), copy.deepcopy(self.__propCompiled)

   def _regProp(self, name, default=None, inherit=False, needed=False, bubble=False, persistent=False):
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

   def resolveLink(self, ids, needChain=False, idsPrepared=False, needChainIsFunc=False):
      stopwatch=self.stopwatch('resolveLink@DBBase')
      if not idsPrepared:
         ids=self._prepIds(ids)
      if needChain and needChainIsFunc is True: _needChain=needChain
      elif needChain is False or needChain is None or not isinstance(needChain, list): needChain=False
      else:
         _needChain=needChain.append
      tQueue=deque((ids, ))
      _tQueueAppend=tQueue.append
      _tQueuePop=tQueue.pop
      _root=self.__index
      while tQueue:
         ids=_tQueuePop()
         branch=_root
         props=None
         for id in ids:
            if id not in branch:
               stopwatch()
               raise BadLinkError(ids)
            props, branch=branch[id]
            if 'link' in props and props['link']:
               _tQueueAppend(props['link'])
         if needChain is not False:
            _needChain((ids, props))
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
      atexit.register(self.close)
      signal.signal(signal.SIGTERM, self.close)
      signal.signal(signal.SIGINT, self.close)

   def _initMeta(self):
      self.__meta={}
      self._loadedMeta(self.__meta)

   def _initIndex(self):
      self.__index={}
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
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      if andReset:
         self._reset()
      self._connect(andReset=andReset, **kwargs)

   def _reset(self):
      self._initMeta()
      self._initIndex()

   def _markInIndex(self, ids, strictMode=True, _changes=None, **props):
      stopwatch=self.stopwatch('_markInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      for k in propRules['needed']:
         if k in props: continue
         if k not in propRules['default']:
            stopwatch()
            raise ValueError('Property "%s" missed and no default value: %s'%(k, ids))
         props[k]=propRules['default'][k]
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
               if not props: pass
               elif _changes is None:
                  branch[id][0].update(props)
               else:
                  tDiff={}
                  dictMergeEx(branch[id][0], props, modify=True, recursive=False, changedCB=tDiff, changedCBPassValues=1)
                  if tDiff:
                     _changes[ids2]=tDiff
            else:
               if not props: branch[id]=({}, {})
               else:
                  tCopy=props.copy()
                  branch[id]=(tCopy, {})
                  if _changes is not None: _changes[ids2]=tCopy
            stopwatch()
            return isExist
         elif not isExist:
            if strictMode:
               stopwatch()
               raise ParentNotExistError('"%s" for %s'%(ids2, ids))
            elif propRules['defaultMinimalProps'] is None:
               stopwatch()
               raise ValueError("Parent '%s' not exists and some property's default vals missed: %s"%(ids2, ids))
            self.workspace.log(2, 'Parent "%s" not exists, creating it with default properties: %s'%(ids2, ids))
            propsCurrent=propRules['defaultMinimalProps'].copy()
            branch[id]=(propsCurrent, {})
         # and now bubbling props if needed (also for missed parents in non-strict mode)
         if propMerger and props:
            tDiff=None if _changes is None else {}
            propMerger(propsCurrent, props, cbArgs=(ids2,), changedCB=tDiff)
            if tDiff:
               _changes[ids2]=tDiff
         branch=branch[id][1]
         stopwatch()

   def _unmarkInIndex(self, ids, _changes=None, **props):
      stopwatch=self.stopwatch('_unmarkInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      if _changes is None or _changes is False or not isinstance(_changes, dict): _changes=None
      branch=self.__index
      for i, id in enumerate(ids):
         if i==iLast: break
         if id not in branch:
            stopwatch()
            return None
         # and now bubbling props if needed (also for missed parents in non-srict mode)
         if propMerger and props:
            ids2=ids[:i+1]
            #? плохо, что в данном случае баблинг происходит даже если удаляемого обьекта несуществует, ибо проверка на его наличие идет позже
            tDiff=None if _changes is None else {}
            propMerger(branch[id][0], props, cbArgs=(ids2,), changedCB=tDiff)
            if tDiff:
               _changes[ids2]=tDiff
         branch=branch[id][1]
      if ids[-1] not in branch:
         stopwatch()
         return False
      del branch[ids[-1]]
      stopwatch()
      return True

   def iterBacklink(self, ids, props=None, recursive=True, treeMode=True, safeMode=True, calcProperties=True, strictMode=True):
      mytime=timetime()
      if props is None:
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
         if not isExist:
            if strictMode:
               raise NotExistError(ids)
            else:
               raise StopIteration
      if not props or 'backlink' not in props or not props['backlink']:
         raise StopIteration
      _soLong=self._settings['iterBranch_soLong']
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.server._sleep
      _timetime=timetime
      _len=len
      _iter=iter
      backlink=props['backlink']
      queue=deque(((_iter(backlink.copy() if safeMode else backlink), backlink),))
      _queueAppend=queue.append
      _queuePop=queue.pop
      while queue:
         iterBacklink, backlink=_queuePop()
         for ids in iterBacklink:
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
            iterBacklinkCurrent=_iter(backlinkCurrent.copy() if safeMode else backlinkCurrent)
            if treeMode:
               _queueAppend((iterBacklink, backlink))
               _queueAppend((iterBacklinkCurrent, backlinkCurrent))
               break
            else:
               _queueAppend((iterBacklinkCurrent, backlinkCurrent))

   def iterBranch(self, ids=None, recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True, skipLinkChecking=False):
      mytime=timetime()
      _soLong=self._settings['iterBranch_soLong']
      _sleepTime=self._settings['iterBranch_sleepTime']
      _sleep=self.workspace.server._sleep
      _timetime=timetime
      _len=len
      _iter=iter
      _resolveLink=self.resolveLink
      propRules=self.__propCompiled
      propMerger=propRules['mergerInherit'] if calcProperties and propRules['inheritCBMap'] else False
      if ids is not None:
         # searching enter-point and calc props if needed
         isExist, propsPre, branchCurrent=self._findInIndex(ids, strictMode=True, calcProperties=propMerger, offsetLast=offsetLast)
         if not isExist:
            raise StopIteration
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
            extCmd=yield ids2, (propsCurrent, _len(branchCurrent))
            if extCmd is not None:
               yield  # this allows to use our generator inside `for .. in ..` without skipping on `send`
               if extCmd is False: continue
            if not recursive: continue
            if not _len(branchCurrent): continue
            iterCurrent=_iter(branchCurrent.keys()) if safeMode else branchCurrent.iterkeys()
            if treeMode:
               _queueAppend((ids, iterParrent, branchParrent, propsParrent))
               _queueAppend((ids2, iterCurrent, branchCurrent, propsCurrent))
               break
            else:
               _queueAppend((ids2, iterCurrent, branchCurrent, propsCurrent))

   def _findInIndex(self, ids, strictMode=False, calcProperties=True, offsetLast=False, needChain=None, skipLinkChecking=False):
      # поиск обьекта в индексе и проверка, существует ли вся его иерархия
      _stopwatch=self.stopwatch
      stopwatch=_stopwatch('_findInIndex%s%s@DBBase'%('-calcProps' if calcProperties else '', '-noLinkCheck' if skipLinkChecking else ''))
      iLast=len(ids)-1
      propRules=self.__propCompiled
      propMerger=propRules['mergerInherit'] if calcProperties and propRules['inheritCBMap'] else False
      if propMerger and propRules['defaultMinimalProps'] is not None:
         tArr1=propRules['defaultMinimalProps'].copy()
      else: tArr1={}
      _root=self.__index
      res=[None, tArr1, _root]
      if propMerger:
         propsQueue=[None]*iLast
      if needChain is False or needChain is None or not isinstance(needChain, list):
         needChain, _needChain=False, None
      else:
         linkChainI=len(needChain)
         _needChain=needChain.append
         needChain.append((ids, None))  # we change it later, when we will have props
      for i, id in enumerate(ids):
         if offsetLast and i==iLast: break
         if id is None and i==iLast:
            # айди не указан, используется при автоматической генерации айди
            res[0]=None
            res[1], res[2]={}, None
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
            self.resolveLink(propsCurrent['link'], needChain=_needChain, idsPrepared=True, needChainIsFunc=True)
         # and now we inherit props if needed
         if propMerger:
            propsQueue[i]=res[1]
         res[1]=propsCurrent
         # if propMerger and res[1]:
         #    propsParrent, res[1]=res[1], propsCurrent.copy() if propsCurrent else {}
         #    propMerger(res[1], propsParrent, cbArgs=(ids[:i+1],))
         # else:
         #    res[1]=(propsCurrent.copy() if propsCurrent else {}) if propMerger else propsCurrent
      # and now we inherit props if needed
      if propMerger:
         res[1]=res[1].copy() if res[1] else {}
         propMerger(res[1], propsQueue, cbArgs=(ids,))
      #
      if needChain is not False:
         needChain[linkChainI]=(ids, res[1])
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
      self.workspace.server._sleep(self._settings['randomEx_sleepTime'])
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

   def link(self, ids, ids2, existChecked=None, onlyIfExist=None, strictMode=False):
      ids=self._prepIds(ids)
      ids2=self._prepIds(ids2)
      return self.set(ids, True, allowMerge=False, existChecked=existChecked, propsUpdate={'link':ids2}, onlyIfExist=onlyIfExist, strictMode=strictMode)

   def remove(self, ids, existChecked=None, strictMode=False):
      self.set(ids, None, allowMerge=False, existChecked=existChecked, onlyIfExist=True, strictMode=strictMode)

   def _validateOnSet(self, ids, data, isExist=None, props=None, allowMerge=None, propsUpdate=None, **kwargs):
      # хук, позволяющий проверить или модефицировать данные (и Props) перед их добавлением
      return isExist, data, allowMerge

   def set(self, ids, data, allowMerge=True, existChecked=None, propsUpdate=None, allowForceRemoveChilds=True, onlyIfExist=None, strictMode=False, **kwargs):
      # если вызывающий хочет пропустить проверку `self._findInIndex()`, нужно передать в `existChecked`, высчитанные `properties` (в таком случае будет считаться, что обьект существует) илиже `(isExist, properties)` если нужно указать непосредственный статус `isExist`
      stopwatch=self.stopwatch('set@DBBase')
      if not propsUpdate: propsUpdate={}
      elif not isinstance(propsUpdate, dict):
         raise ValueError('Incorrect type, `propsUpdate` must be a dict')
      if data is not None and data is not True and data is not False and not isDict(data):
         raise ValueError('Incorrect data format')
      ids=self._prepIds(ids)
      if existChecked is None:
         isExist, props, _=self._findInIndex(ids, strictMode=True)
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
         return None
      if data is not True and 'link' in props and props['link']: propsUpdate['link']=None
      #
      _backlinkDel, _oldLink, _backlinkAdd, _newLink=False, None, False, None
      if 'link' in propsUpdate:
         stopwatch1=self.stopwatch('set.fixLink@DBBase')
         _newLink=propsUpdate['link']
         if 'link' in props and props['link']:
            _oldLink=props['link']
            if _oldLink==_newLink:
               del propsUpdate['link']
            else:
               if _newLink is not None:
                  try:
                     badLinkChain=[]
                     self.resolveLink(_newLink, needChain=badLinkChain.append, idsPrepared=True, needChainIsFunc=True)
                  except BadLinkError, e:
                     # удаляем плохой линк
                     for ids, props in reversed(badLinkChain):
                        self.set(ids, None, existChecked=props, allowForceRemoveChilds=True)
                     stopwatch1()
                     stopwatch()
                     raise e
                  _backlinkAdd=True
               _backlinkDel=True
         elif _newLink is not None:
            try:
               badLinkChain=[]
               self.resolveLink(_newLink, needChain=badLinkChain.append, idsPrepared=True, needChainIsFunc=True)
            except BadLinkError, e:
               # удаляем плохой линк
               for ids, props in reversed(badLinkChain):
                  self.set(ids, None, existChecked=props, allowForceRemoveChilds=True)
               stopwatch1()
               stopwatch()
               raise e
            _backlinkAdd=True
         stopwatch1()
      #
      isExist, data, allowMerge=self._validateOnSet(ids, data, isExist=isExist, props=props, allowMerge=allowMerge, propsUpdate=propsUpdate)
      if data is None and not isExist:
         stopwatch()
         return ids
      elif data is not False:
         tArr=((ids, (isExist, data, allowMerge, props, propsUpdate)),)
         self._set(tArr, **kwargs)
      if data is None:
         if self._settings['force_removeChilds'] and allowForceRemoveChilds:
            stopwatch1=self.stopwatch('set.removeChilds@DBBase')
            cNeedProps=self._settings['force_removeChilds_calcProps']
            _backlinkDelMap=defaultdict(list)
            _backlinkIgnoreMap=set()
            # принудительно удаляем детей, чтобы избежать конфликта при повторном использовании техже имен
            for idsC, (propsC, l) in self.iterBranch(ids=ids, recursive=True, treeMode=False, safeMode=True, calcProperties=cNeedProps):
               tArr=((idsC, (True, None, False, propsC, {})),)
               self._set(tArr, **kwargs)
               if 'link' in propsC and propsC['link'] and propsC['link'] not in _backlinkIgnoreMap:
                  _backlinkDelMap[propsC['link']].append(idsC)
               _backlinkIgnoreMap.add(idsC)
               if idsC in _backlinkDelMap:
                  del _backlinkDelMap[idsC]
            # updating back-link for sub-branches
            for ids1, tArr1 in _backlinkDelMap.iteritems():
               tArr=self._findInIndex(ids1, strictMode=True, calcProperties=False)[1]['backlink']
               for s in tArr1: tArr.remove(s)
               self._markInIndex(ids1, backlink=tArr)
            stopwatch1()
         self._unmarkInIndex(ids, **propsUpdate)
      elif not isExist or propsUpdate:
         self._markInIndex(ids, **propsUpdate)
      # updating back-link
      if _backlinkAdd:
         stopwatch1=self.stopwatch('set.backlinkAdd@DBBase')
         tArr=self._findInIndex(_newLink, strictMode=True, calcProperties=False)[1]
         tArr=tArr['backlink'] if 'backlink' in tArr else set()
         tArr.add(ids)
         self._markInIndex(_newLink, backlink=tArr)
         stopwatch1()
      if _backlinkDel:
         stopwatch1=self.stopwatch('set.backlinkDel@DBBase')
         tArr=self._findInIndex(_oldLink, strictMode=True, calcProperties=False)[1]['backlink']
         tArr.remove(ids)
         self._markInIndex(_oldLink, backlink=tArr)
         stopwatch1()
      stopwatch()
      return ids

   def _set(self, items, **kwargs):
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
            isExist, props, _=self._findInIndex(ids, strictMode=strictMode, calcProperties=True, needChain=badLinkChain)
            ids=badLinkChain[-1][0]  # getting target, if this was link
         else:
            isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
            ids=self.resolveLink(ids, needChain=badLinkChain.append, idsPrepared=True, needChainIsFunc=True)
      except BadLinkError, e:
         if strictMode:
            stopwatch()
            raise StrictModeError(e)
         # удаляем плохой линк
         for ids, props in reversed(badLinkChain):
            self.set(ids, None, existChecked=props, allowForceRemoveChilds=True)
         stopwatch()
         return None
      if not isExist:
         stopwatch()
         if strictMode:
            raise NotExistError(ids)
         return None
      res=self._get(ids, props, **kwargs)
      if res is not True and not returnRaw:
         if self._settings['return_frozen']:
            res=MagicDictCold(res)
            res._MagicDictCold__freeze()
         else:
            res=MagicDict(res)
      stopwatch()
      return res

   def _get(self, ids, props, **kwargs):
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
