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

#! дать возможность включать py-sortedcontainers:sortedDict (или его вариации через модуль sortedcollections) в качестве замены для хранилища веток. это позволит строить индексы для ускорения поиска прямо внутри базы данных
#! чтобы использовать базу данных в том числе и для построения собственных индексов, нужно добавить возможность исключать ветку из события Store и из iterIndex. Логичнее всего сделать это через дополнительный Prop (возможно наследуемый)
#! подготовить self._branchLock выставляемый для веток при записи. это позволит выполнять специфичные операции записи (такие как удаление всей ветки рекурсивно) в фоне - достаточно удалить целевой обьект из индекса и выставить блокировку. после чего остальные операции удаления можно делать в фоне (предварительно сделав автономную копию итератора по веткуе). и тогда параллельно можно выполнять операции чтения, и даже записи (но в другие ветки естественно). этот механизм нужно сделать не через Props ибо он должен уметь работать для удаленной из индекса ветки
#! нужен встроенный механизм для background задачь с эмуляцией асинхронности
#! убедиться, что логика работы дефолтных значений для Props непротеворечива. иначе говоря если дефолтное значение задано то в случае пустого Prop должно возвращаться именно дефолтное значение (если не включено и не произошло наследование)
#! доделать механизм стоп-паттернов в имени айдишника, смотри метод `_inited()`
#? былобы здорово добавить нормализацию Props - если задано дефолтное значение, и переданное значение равно ему, нет смысла записывать его в индекс
#! сделать расширение, реализующее концепцию дискретов и использующее ленивые вычисления и всплывающую Prop
#? масштабирование через перенос префиксов реализуется крайне тривиально - введением нового Prop (без дополнительных свойств) и его проверкой при обращении к индексу. единственный нюанс - проверка этого Prop и соответствующие действия при его наличии происходят гдето посреди кода соответствующих методов, в расширением вклиниться туда неполучится. нужно либо реализовывать базовую обработку и хук на уровне ядра, либо разбивать соответствующие методы на части (одной из которых будет обработку текущего обьекта в иерархии)

class DBBase(object):
   def __init__(self, workspace, *args, **kwargs):
      self.version=__version__
      self._main_app=sys.modules['__main__']
      self.inited=False
      self.workspace=workspace
      self._speedStats=defaultdict(lambda:deque2(maxlen=99999))
      self._speedStatsMax=defaultdict(int)
      self._idBadPattern=[]
      self.settings=MagicDict({})
      self.supports=MagicDict({})
      self._propMap={}
      self._propCompiled={
         'inheritCBMap':{},
         'bubble':set(), 'bubbleCBMap':{},
         'needed':set(),
         'default':{},
         'persistent':set(),
         'defaultMinimalProps':{},
         'mergerBubble':None,
         'mergerInherit':None,
      }
      self._propCompiled['mergerBubble']=bind(dictMergeEx, {
         'modify':True,
         'recursive':False,
         'cbMap':self._propCompiled['bubbleCBMap'],
         'cbPassKey':True,
         'cbSkipIfNewKey':False,
         'filterKeys':self._propCompiled['bubble'],
         'isBlacklist':False,
         'changedCBPassValues':1,
      })
      self._propCompiled['mergerInherit']=bind(dictMergeEx, {
         'modify':True,
         'recursive':False,
         'cbMap':self._propCompiled['inheritCBMap'],
         'cbPassKey':True,
         'cbSkipIfNewKey':False,
         'filterKeys':self._propCompiled['inheritCBMap'],
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
         'prop_link':True,
      }
      self._branchLock={}
      #
      kwargs2=self._init(*args, **kwargs)
      kwargs2=kwargs2 if isDict(kwargs2) else {}
      self._inited(**kwargs2)

   def _init(self, *args, **kwargs):
      self._regProp('link', persistent=True)
      #
      for k, v in self.__supportsDefault.iteritems():
         self.supports.setdefault(k, v)
      #
      self.settings.iterIndex_soLong=0.3
      self.settings.iterIndex_sleepTime=0.000001  # not null, becouse this allows to switch to another io-ready greenlet
      self.settings.randomEx_soLong=0.05
      self.settings.randomEx_maxAttempts=9
      self.settings.randomEx_sleepTime=0.001
      self.settings.return_frozen=True
      self.settings.merge_ex=True
      self.settings.force_removeChilds=True
      self.settings.force_removeChilds_calcProps=False
      return kwargs

   def _inited(self, autoLoad=True, **kwargs):
      self.inited=True
      # tArr=set(self._idBadPattern)
      # self._idBadPattern=None
      self._initPreExit()
      if autoLoad:
         self.load(andReset=True)
      else:
         self._reset()

   def stopwatch(self, name):
      mytime=timetime()
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

   def _stats(self, **kwargs):
      speedTree=defaultdict(dict)
      speedFlat=defaultdict(lambda: (0,)*5)
      for k, v in self._speedStats.iteritems():
         name, ext=k.split('@', 1)
         val=(len(v), min(v), arrMedian(v), max(v), self._speedStatsMax[k])
         speedTree[name][ext]=val
         speedFlat[name]=tuple(speedFlat[name][i]+s if i else max(speedFlat[name][i], s) for i,s in enumerate(val))
      return speedTree, speedFlat

   def stats(self, returnRaw=False):
      speedTree, speedFlat=self._stats()
      if returnRaw:
         return {
            'speedstatsFlat':speedFlat,
            'speedstatsTree':speedTree,
         }
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

   def _regProp(self, name, default=None, inherit=False, needed=False, bubble=False, persistent=False):
      assert isStr(name), 'Name must be string'
      if self.inited:
         raise RuntimeError('Registering of property not allowed, when DB inited')
      if name in self._propMap:
         raise ValueError('Property with this name already registered')
      o=MagicDictCold({'name':name, 'default':default, 'inherit':inherit, 'needed':needed, 'bubble':bubble, 'persistent':persistent})
      self._propMap[name]=o
      if o.default is not None:
         self._propCompiled['default'][name]=o.default
      if o.inherit:
         self._propCompiled['inheritCBMap'][name]=self._convPropCB_inherit(o)
      if o.bubble:
         self._propCompiled['bubble'].add(name)
         if isFunction(o.bubble):
            self._propCompiled['bubbleCBMap'][name]=o.bubble
      if o.needed:
         self._propCompiled['needed'].add(name)
      if o.persistent:
         self._propCompiled['persistent'].add(name)
      o._MagicDictCold__freeze()
      # compiling defaultMinimalProps
      self._propCompiled['defaultMinimalProps']={}
      for k in set(itertools.chain(self._propCompiled['needed'], self._propCompiled['default'])):
         o=self._propMap[k]
         if k in self._propCompiled['needed'] and k not in self._propCompiled['default']: break
         self._propCompiled['defaultMinimalProps'][k]=self._propCompiled['default'][k]
      else:
         self._propCompiled['defaultMinimalProps']=None

   def _convPropCB_inherit(self, o):
      f=o.inherit
      if f is True:
         o.inheritCB=lambda k, vSelf, vParrent, ids: vParrent
      elif f=='and' or f=='and+':
         o.inheritCB=lambda k, vSelf, vParrent, ids: vParrent and vSelf
      elif f=='+and':
         o.inheritCB=lambda k, vSelf, vParrent, ids: vSelf and vParrent
      elif f=='or' or f=='or+':
         o.inheritCB=lambda k, vSelf, vParrent, ids: vParrent or vSelf
      elif f=='+or':
         o.inheritCB=lambda k, vSelf, vParrent, ids: vSelf or vParrent
      elif isFunction(f): o.inheritCB=f
      else:
         raise ValueError('Unsupported value for inherit-callback of Property "%s": %r'%(o.name, f))
      return o.inheritCB

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
      if isExist:
         # just checking for links (if this is link ofc)
         self.resolveLink(ids, props=props, calcProperties=False)
      stopwatch()
      return ids, isExist, props

   def isExist(self, ids):
      try:
         return self.checkIds(ids)[1]
      except BadLinkError:
         return False

   def resolveLink(self, ids, props=None, calcProperties=False, needChain=False):
      stopwatch=self.stopwatch('resolveLink%s@DBBase'%('-calcProps' if calcProperties else ''))
      ids=self._prepIds(ids)
      if not props:
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
         if not isExist:
            stopwatch()
            return None, None
      if not isinstance(needChain, list): needChain=False
      else:
         needChain.append((ids, props))
      while 'link' in props and props['link']:
         #! нужна защита от циклических ссылок
         ids=props['link']
         isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=calcProperties)
         if not isExist:
            stopwatch()
            raise BadLinkError('Refering to non-existed obj: %s'%(ids,))
         if needChain is not False:
            needChain.append((ids, props))
      stopwatch()
      return ids, props

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
      self.__meta=MagicDict({})
      self._loadedMeta(self.__meta)

   def _initIndex(self):
      self.__index={}
      self._loadedIndex(self.__index)

   def _loadedMeta(self, data):
      pass

   def _loadedIndex(self, data):
      pass

   def load(self, andReset=True, **kwargs):
      if andReset:
         self._reset()

   def _reset(self):
      self._initMeta()
      self._initIndex()

   def _markInIndex(self, ids, strictMode=True, _changes=None, **kwargs):
      stopwatch=self.stopwatch('_markInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self._propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      props=kwargs
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
               raise StrictModeError('Parent "%s" not exists: %s'%(ids2, ids))
            elif propRules['defaultMinimalProps'] is None:
               stopwatch()
               raise ValueError("Parent '%s' not exists and some property's default vals missed: %s"%(ids2, ids))
            self.workspace.log(2, 'Parent "%s" not exists, creating it with default properties: %s'%(ids2, ids))
            branch[id]=(propRules['defaultMinimalProps'].copy(), {})
         # and now bubbling props if needed (also for missed parents in non-strict mode)
         if propMerger and props:
            tDiff=None if _changes is None else {}
            propMerger(branch[id][0], props, cbArgs=(ids2,), changedCB=tDiff)
            if tDiff:
               _changes[ids2]=tDiff
         branch=branch[id][1]
         stopwatch()

   def _unmarkInIndex(self, ids, _changes=None, **kwargs):
      stopwatch=self.stopwatch('_unmarkInIndex@DBBase')
      iLast=len(ids)-1
      propRules=self._propCompiled
      propMerger=propRules['mergerBubble'] if propRules['bubble'] else False
      props=kwargs
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

   def iterIndex(self, ids=None, recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True):
      mytime=timetime()
      soLong=self.settings['iterIndex_soLong']
      sleepTime=self.settings['iterIndex_sleepTime']
      propRules=self._propCompiled
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
      tQueue=deque(((ids, iter(branchCurrent.keys()) if safeMode else branchCurrent.iterkeys(), branchCurrent, propsPre),))
      # testMap=defaultdict(int)
      while tQueue:
         ids, iterParrent, branchParrent, propsParrent=tQueue.pop()
         # testMap[ids]+=1
         for id in iterParrent:
            if soLong and timetime()-mytime>=soLong:
               self.workspace.server._sleep(sleepTime)
               mytime=timetime()
            if id not in branchParrent: continue
            propsCurrent, branchCurrent=branchParrent[id]
            ids2=ids+(id,) if ids else (id,)
            # and now we inherit props if needed
            if propMerger and propsParrent:
               propsCurrent=propsCurrent.copy() if propsCurrent else {}
               propMerger(propsCurrent, propsParrent, cbArgs=(ids2,))
            else:
               propsCurrent=(propsCurrent.copy() if propsCurrent else {}) if propMerger else propsCurrent
            extCmd=yield ids2, (propsCurrent, len(branchCurrent))
            if extCmd is not None:
               yield  # this allows to use our generator inside `for .. in ..` without skipping on `send`
               if extCmd is False: continue
            if not recursive: continue
            if not len(branchCurrent): continue
            iterCurrent=iter(branchCurrent.keys()) if safeMode else branchCurrent.iterkeys()
            if treeMode:
               tQueue.append((ids, iterParrent, branchParrent, propsParrent))
               tQueue.append((ids2, iterCurrent, branchCurrent, propsCurrent))
               break
            else:
               tQueue.append((ids2, iterCurrent, branchCurrent, propsCurrent))
      # print '!!!', {k:v for k,v in testMap.iteritems() if v>1}

   def _findInIndex(self, ids, strictMode=False, calcProperties=True, offsetLast=False):
      # поиск обьекта в индексе и проверка, существует ли вся его иерархия
      stopwatch=self.stopwatch('_findInIndex%s@DBBase'%('-calcProps' if calcProperties else ''))
      iLast=len(ids)-1
      propRules=self._propCompiled
      propMerger=propRules['mergerInherit'] if calcProperties and propRules['inheritCBMap'] else False
      if propMerger and propRules['defaultMinimalProps'] is not None:
         tArr1=propRules['defaultMinimalProps'].copy()
      else: tArr1={}
      res=[None, tArr1, self.__index]
      if propMerger:
         propsQueue=[None]*iLast
      for (i, id) in enumerate(ids):
         if offsetLast and i==iLast: break
         if id is None and i==iLast:
            # айди не указан, используется при автоматической генерации айди
            res[0]=None
            break
         if id not in res[2]:
            if i==iLast: res[0]=False
            else:
               if strictMode:
                  stopwatch()
                  raise StrictModeError('Parent "%s" not exists: %s'%(id, ids))
               res[0]=None
            break
         res[0]=True
         propsCurrent, res[2]=res[2][id]
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
      stopwatch()
      return res

   def _parseMeta(self, data):
      data=pickle.loads(data)
      if not isDict(data):
         raise TypeError('Meta must be a dict')
      self.__meta=MagicDict(data)
      self._loadedMeta(self.__meta)

   def _dumpMeta(self):
      return pickle.dumps(dict(self.__meta))

   def _saveMetaToStore(self, **kwargs):
      pass

   def _dictMerge(self, o1, o2, changed=None, changedType='key', **kwargs):
      stopwatch=self.stopwatch('_dictMerge@DBBase')
      if self.settings['merge_ex']:
         dictMerge(o1, o2, changed=changed, changedType=changedType, modify=True)
         stopwatch()
         return True
      else:
         o1.update(o2)
         stopwatch()

   def randomExSoLongCB(self, mult, vals, pref, suf, i):
      if i>self.settings['randomEx_maxAttempts']:
         self.workspace.log(0, 'randomEx generating value so long for (%s, %s, %s), %s attempts, aborted'%(pref, mult, suf, i))
         raise OverflowError('randomeEx generating so long (%s attempts)'%i)
      self.workspace.log(2, 'randomEx generating value so long for (%s, %s, %s), attempt %s'%(pref, mult, suf, i))
      self.workspace.server._sleep(self.settings['randomEx_sleepTime'])
      return mult

   def _generateId(self, ids, offsetLast=True, **kwargs):
      # создает новый айди для новой сущности
      stopwatch=self.stopwatch('_generateId@DBBase')
      ids=ids[:-1] if offsetLast else ids
      tArr=self._findInIndex(ids, strictMode=True, calcProperties=False, offsetLast=False)
      newId=randomEx(vals=tArr, soLong=self.settings['randomEx_soLong'], cbSoLong=self.randomExSoLongCB)
      ids+=(newId,)
      stopwatch()
      return ids

   def link(self, ids, ids2, existChecked=None):
      ids=self._prepIds(ids)
      ids2=self.checkIds(ids2)[0]
      return self.set(ids, True, allowMerge=False, existChecked=existChecked, propsUpdate={'link':ids2})

   def remove(self, ids, existChecked=None):
      self.set(ids, None, allowMerge=False, existChecked=existChecked)

   def _validateOnSet(self, ids, data, isExist=None, props=None, allowMerge=None, propsUpdate=None, **kwargs):
      # хук, позволяющий проверить или модефицировать данные (и Props) перед их добавлением
      return data

   def set(self, ids, data, allowMerge=True, existChecked=None, propsUpdate=None, allowForceRemoveChilds=True, **kwargs):
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
      if data is not True and 'link' in props and props['link']: propsUpdate['link']=None
      data=self._validateOnSet(ids, data, isExist=isExist, props=props, allowMerge=allowMerge, propsUpdate=propsUpdate)
      if data is None and not isExist:
         stopwatch()
         return
      elif data is not False:
         tArr=((ids, (isExist, data, allowMerge, props, propsUpdate)),)
         self._set(tArr, **kwargs)
      if data is None:
         if self.settings['force_removeChilds'] and allowForceRemoveChilds:
            cNeedProps=self.settings['force_removeChilds_calcProps']
            # принудительно удаляем детей, чтобы избежать конфликта при повторном использовании техже имен
            for idsC, (propsC, l) in self.iterIndex(ids=ids, recursive=True, treeMode=False, safeMode=True, calcProperties=cNeedProps):
               tArr=((idsC, (True, None, False, propsC, {})),)
               self._set(tArr, **kwargs)
         self._unmarkInIndex(ids, **propsUpdate)
      elif not isExist or propsUpdate:
         self._markInIndex(ids, **propsUpdate)
      stopwatch()

   def _set(self, items, **kwargs):
      pass

   def get(self, ids, existChecked=None, returnRaw=False, strictMode=False, **kwargs):
      ids=self._prepIds(ids)
      # если вызывающий хочет пропустить проверку `self._findInIndex()`, нужно передать в `existChecked`, высчитанные `properties` (в таком случае будет считаться, что обьект существует) илиже `(isExist, properties)` если нужно указать непосредственный статус `isExist`
      stopwatch=self.stopwatch('get@DBBase')
      if existChecked is None:
         isExist, props, _=self._findInIndex(ids, strictMode=strictMode)
      elif isinstance(existChecked, tuple):
         isExist, props=existChecked
      else:
         isExist, props=True, existChecked
      #
      if not isExist:
         stopwatch()
         return None
      badLinkChain=[]
      try:
         ids, props=self.resolveLink(ids, props, calcProperties=True, needChain=badLinkChain)
      except BadLinkError, e:
         if strictMode:
            stopwatch()
            raise StrictModeError(e)
         # удаляем плохой линк
         for ids, props in badLinkChain:
            self.remove(ids, existChecked=props)
         stopwatch()
         return None
      res=self._get(ids, props, **kwargs)
      if res is not True and not returnRaw:
         if self.settings['return_frozen']:
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

   def close(self, **kwargs):
      self.workspace.log(4, 'Closing db')

   def __enter__(self):
      return self

   def __exit__(self):
      self.close()
