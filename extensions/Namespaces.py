# -*- coding: utf-8 -*-
__ver_major__ = 0
__ver_minor__ = 2
__ver_patch__ = 0
__ver_sub__ = "dev"
__version__ = "%d.%d.%d" % (__ver_major__, __ver_minor__, __ver_patch__)
"""
:authors: John Byaka
:copyright: Copyright 2019, Buber
:license: Apache License 2.0

:license:

   Copyright 2019 Buber

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from ..utils import *
from ..DBBase import DBBase

def __init():
   return DBNamespaced, ('Namespaces', 'NS')

class NamespaceError(BaseDBErrorPrefixed):
   """Namespace-related error"""

class NamespaceOrderError(NamespaceError):
   """Incorrect namespaces order"""

class NamespaceIndexError(NamespaceError):
   """Incorrect index"""

class DBNamespaced(DBBase):
   def _init(self, *args, **kwargs):
      res=super(DBNamespaced, self)._init(*args, **kwargs)
      self._idBadPattern.add(BadPatternStarts('?'))
      self.supports.namespaces=True
      self.settings.ns_checkIndexOnUpdateNS=True
      self.settings.ns_checkIndexOnConnect=True
      self.settings.ns_config_keyMap=['parent', 'child', ('onlyIndexed', True)]
      self.settings.ns_parseId_allowOnlyName=True
      self.namespace_pattern_parse=re.compile(r'^([a-zA-Z_]+)(\d+|[\-\.\s#$@].+)$')
      self.namespace_pattern_check=re.compile(r'^[a-zA-Z_]+$')
      return res

   def _initNS(self, data=None):
      if '_namespace' not in data: data['_namespace']={}
      self.__ns=data['_namespace']
      self._repareNSConfig()
      #! хранение дефолтных значений внутри конфига не позволяет запустить туже базу с другим набором дефолтных параметров
      self._loadedNS(self.__ns)

   def _repareNSConfig(self, keyMap=None):
      keyMap=keyMap or self._settings['ns_config_keyMap']
      _required=object()
      for ns, nso in self.__ns.iteritems():
         for k in keyMap:
            if isString(k):
               vdef=_required
            elif isTuple(k) and len(k)==2:
               k, vdef=k
            else:
               raise ValueError('Unknown config for Namespaces: %s'%k)
            if k in nso: continue
            if vdef is _required:
               raise KeyError('Missed required config-key for NS(%s)'%ns)
            nso[k]=vdef

   def _connect(self, **kwargs):
      super(DBNamespaced, self)._connect(**kwargs)
      if self._settings['ns_checkIndexOnConnect']:
         if not self.__ns:
            self.workspace.log(2, 'Namespaces not configured!')
         self._checkIndexForNS(calcMaxIndex=True)

   def _loadedNS(self, data):
      pass

   def _loadedMeta(self, data):
      self._initNS(data)
      return super(DBNamespaced, self)._loadedMeta(data)

   def _checkIdsNS(self, ids, nsIndexMap=None, props=None, **kwargs):
      stopwatch=self.stopwatch('_checkIdsNS@DBNamespaced')
      nsMap=self.__ns
      if not nsMap: return None
      tArr=[]
      nsPrev, nsoPrev=None, None
      for idNow in ids:
         nsNow, nsi=self._parseId2NS(idNow)
         if (nsoPrev and nsoPrev['child'] and (
            (nsoNow and nsNow not in nsoPrev['child']) or
            (not nsoNow and None not in nsoPrev['child']))):
            stopwatch()
            raise NamespaceOrderError('NS(%s) cant be child of NS(%s)'%(nsNow, nsPrev))
         nsoNow=None
         if nsNow and nsNow in nsMap:
            nsoNow=nsMap[nsNow]
            if nsIndexMap is not None:
               nsIndexMap.setdefault(nsNow, [])
               nsIndexMap[nsNow].append(nsi)
            if nsoNow['parent'] and (
               (nsoPrev and nsPrev not in nsoNow['parent']) or
               (not nsoPrev and None not in nsoNow['parent'])):
               stopwatch()
               raise NamespaceOrderError('NS(%s) cant be parent of NS(%s)'%(nsPrev, nsNow))
         nsPrev, nsoPrev=nsNow, nsoNow
         tArr.append((idNow, nsNow, nsi, nsoNow))
      stopwatch()
      return tArr

   def _checkIndexForNS(self, calcMaxIndex=True):
      #! разным расширениям зачастую нужен механизм обхода всех обьектов при инициализации. нет смысла делать это раздельно, нужен единый механизм
      #? однако этаже функция используется при реконфигурации неймспейсов для выполнения валидации.
      nsMap=self.__ns
      if not nsMap: return
      if calcMaxIndex and calcMaxIndex is not True:
         calcMaxIndex=calcMaxIndex if isList(calcMaxIndex) else (list(calcMaxIndex) if isTuple(calcMaxIndex) else [calcMaxIndex])
         calcMaxIndex=[ns for ns in calcMaxIndex if ns in nsMap]
      for ids, (props, l) in self.iterBranch(recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True):
         tArr={}
         self._checkIdsNS(ids, nsIndexMap=tArr, props=props, childCount=l)
         if not calcMaxIndex: continue
         for ns, iiArr in tArr.iteritems():
            if calcMaxIndex is not True and ns not in calcMaxIndex: continue
            nso=nsMap[ns]
            if not nso['maxIndex']: nso['maxIndex']=0
            nsiMax=nso['maxIndex']
            for nsi in iiArr:
               try: nsi=int(nsi)
               except (ValueError, TypeError):
                  if nso['onlyIndexed']:
                     self.workspace.log(2, 'Incorrect index for id (%s%s)'%(ns, nsi))
                  continue
               nsiMax=max(nsiMax, nsi)
            nso['maxIndex']=nsiMax

   def _parseId2NS(self, id, needNSO=False):
      stopwatch=self.stopwatch('_parseId2NS@DBNamespaced')
      res=self.namespace_pattern_parse.search(id)
      if res is None:
         if self._settings['ns_parseId_allowOnlyName'] and self.namespace_pattern_check.match(id) is not None:
            res=(id, None)
         else:
            res=(None, None)
      else:
         res=(res.group(1), res.group(2))
      if needNSO:
         ns, nsMap=res[0], self.__ns
         res+=(nsMap[ns] if (ns is not None and ns in nsMap) else None,)
      stopwatch()
      return res

   def _generateIdNS(self, ns):
      stopwatch=self.stopwatch('_generateIdNS@DBNamespaced')
      nsMap=self.__ns
      if not nsMap or ns not in nsMap: return None
      nso=nsMap[ns]
      ii=nso['maxIndex'] or 0
      ii+=1
      nso['maxIndex']=ii
      id='%s%i'%(ns, ii)
      stopwatch()
      return id

   def ids2ns_generator(self, ids):
      assert isinstance(ids, (tuple, list))
      for id in ids:
         ns, nsi, nso=self._parseId2NS(id, needNSO=True)
         yield (id, ns, nsi, nso)

   def ids2ns(self, ids):
      return tuple(self.ids2ns_generator(ids))

   def _namespaceChanged(self, ns, nsoNow, nsoOld):
      pass

   def configureNS(self, config, andClear=True, keyMap=None):
      if andClear:
         old=self.__ns.copy()
         self.__ns.clear()
         for ns, nsoOld in old.iteritems():
            self._namespaceChanged(ns, None, nsoOld)
      #! добавить поддержку разных форматов конфига с авто-конвертом в дефолтный
      keyMap=keyMap or self._settings['ns_config_keyMap']
      tArr1=[]
      for ns, vals in config:
         params={}
         for i, k in enumerate(keyMap):
            if isString(k):  #required
               if i>=len(vals):
                  raise IndexError('No value for key "%s"'%k)
               params[k]=vals[i]
            elif isTuple(k) and len(k)==2:
               k, vdef=k
               params[k]=vdef if i>=len(vals) else vals[i]
         self.setNS(ns, allowCheckIndex=False, **params)
         tArr1.append(ns)
      if self._settings['ns_checkIndexOnUpdateNS']:
         self._checkIndexForNS(calcMaxIndex=(True if andClear else tArr1))

   def setNS(self, ns, parent=None, child=None, onlyIndexed=True, allowCheckIndex=True, **kwargs):
      if not isString(ns) or not self.namespace_pattern_check.match(ns):
         raise ValueError("Incorrect format for NS's name: '%s'"%(ns,))
      #! arg's type-checking
      if parent is not None:
         parent=parent if isTuple(parent) else (tuple(parent) if isList(parent) else [parent])
      if child is not None:
         child=child if isTuple(child) else (tuple(child) if isList(child) else [child])
      nsMap=self.__ns
      nsoOld={}
      if ns in nsMap:
         nsoOld=nsMap[ns]
      nsoNow={'parent':parent, 'child':child, 'onlyIndexed':onlyIndexed, 'maxIndex':nsoOld.get('maxIndex', 0)}
      nsoNow.update(kwargs)  # позволяет хранить в namespace дополнительные данные
      if not nsoOld or nsoOld!=nsoNow:
         nsMap[ns]=nsoNow
         self._namespaceChanged(ns, nsoNow, nsoOld)
         if allowCheckIndex and self._settings['ns_checkIndexOnUpdateNS']: self._checkIndexForNS(calcMaxIndex=ns)

   def delNS(self, ns, strictMode=True, allowCheckIndex=True):
      if not isString(ns) or not self.namespace_pattern_check.match(ns):
         raise ValueError('Incorrect format for NS name: "%s"'%(ns,))
      nsMap=self.__ns
      if ns not in nsMap:
         if strictMode:
            raise StrictModeError('Namespace "%s" not exist'%ns)
         return
      nso=nsMap.pop(ns)
      self._namespaceChanged(ns, None, nso)
      if allowCheckIndex and self._settings['ns_checkIndexOnUpdateNS']: self._checkIndexForNS(calcMaxIndex=False)

   def _validateOnSetNS(self, ids, data, lastId, nsPrev, nsoPrev, nsMap, **kwargs):
      stopwatch=self.stopwatch('_validateOnSetNS@DBNamespaced')
      if lastId is None:
         if nsoPrev and nsoPrev['child'] and None not in nsoPrev['child']:
            stopwatch()
            raise NamespaceOrderError('NS(%s) cant be child of NS(%s)'%(None, nsPrev))
         return None, None, None
      else:
         nsNow, nsi=self._parseId2NS(lastId)
         nsoNow=None
         if nsNow and nsNow in nsMap:
            nsoNow=nsMap[nsNow]
            #! такой способ конвертации довольно дорогой. лучше в парсере пытаться отдельно извлечь цифровую группу и отдельно строковый идентификатор, и таким образом выполнять конвертацию прямо в парсере
            try: nsi=int(nsi)
            except (ValueError, TypeError):
               if nsoNow['onlyIndexed']:
                  stopwatch()
                  raise NamespaceIndexError('index required for NS(%s)'%(nsNow,))
            #! поскольку у нас нет отдельного метода для добавления и отдельного для редактирования, проверка на индексы бесполезна
            # if isinstance(nsi, int) and nsi>(nsoNow['maxIndex'] or 0):
            #    stopwatch()
            #    raise NamespaceIndexError('too large value %s for NS(%s)'%(nsi, nsNow))
            if nsoNow['parent'] and (
               (nsoPrev and nsPrev not in nsoNow['parent']) or
               (not nsoPrev and None not in nsoNow['parent'])):
               stopwatch()
               raise NamespaceOrderError('NS(%s) cant be parent of NS(%s)'%(nsPrev, nsNow))
         if (nsoPrev and nsoPrev['child'] and (
            (nsoNow and nsNow not in nsoPrev['child']) or
            (not nsoNow and None not in nsoPrev['child']))):
            stopwatch()
            raise NamespaceOrderError('NS(%s) cant be child of NS(%s)'%(nsNow, nsPrev))
         stopwatch()
         return nsNow, nsi, nsoNow

   def set(self, ids, data, allowMerge=True, existChecked=None, onlyIfExist=None, strictMode=False, **kwargs):
      stopwatch=self.stopwatch('set@DBNamespaced')
      ids=ids if isinstance(ids, list) else(list(ids) if isinstance(ids, tuple) else [ids])
      lastId=ids[-1]
      nsMap=self.__ns
      isExist, props=False, {}
      if lastId is None: pass  # default newId-generator will be invoked
      elif lastId[0]=='?':
         # namespace-based newId-generator
         ns=lastId[1:]
         lastId=ids[-1]=self._generateIdNS(ns)
      else:
         if existChecked is None:
            isExist, props, _=self._findInIndex(ids, strictMode=True)
         else:
            isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
         if onlyIfExist is not None and isExist!=onlyIfExist:
            if strictMode:
               raise ExistStatusMismatchError('expected "isExist=%s" for %s'%(onlyIfExist, ids))
            return None
      nsPrev=self._parseId2NS(ids[-2])[0] if len(ids)>1 else None
      nsoPrev=nsMap[nsPrev] if (nsPrev and nsPrev in nsMap) else None
      # namespace-rules checking
      nsNow, nsi, nsoNow=self._validateOnSetNS(ids, data, lastId, nsPrev, nsoPrev, nsMap, isExist=isExist, props=props, allowMerge=allowMerge, **kwargs)
      if 'backlink' in props and props['backlink']:
         # also checking namespace-rules for backlinks
         _queue=deque(props['backlink'])
         _queuePop=_queue.pop
         _queueExtend=_queue.extend
         _findInIndex=self._findInIndex
         _parseId2NS=self._parseId2NS
         _validateOnSetNS=self._validateOnSetNS
         while _queue:
            ids2=_queuePop()
            badLinkChain=[]
            try:
               isExist2, props2, _=_findInIndex(ids2, strictMode=True, calcProperties=False, skipLinkChecking=True, needChain=badLinkChain)
            except BadLinkError:
               # удаляем плохой линк
               #? почему здесь полное удаление, а не через прощенный вариант как в `self.set()`
               for _ids, _props in reversed(badLinkChain):
                  self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
               continue
            nsPrev2=_parseId2NS(ids2[-2])[0] if len(ids2)>1 else None
            nsoPrev2=nsMap[nsPrev2] if (nsPrev2 and nsPrev2 in nsMap) else None
            _validateOnSetNS(ids2, data, ids2[-1], nsPrev2, nsoPrev2, nsMap, isExist=isExist2, props=props2, allowMerge=allowMerge, **kwargs)
            if 'backlink' in props2 and props2['backlink']:
               _queueExtend(props2['backlink'])
      # all checkings passed
      ids=tuple(ids)
      needReplaceMaxIndex=(nsoNow and nsi and data is not None and data is not False and isinstance(nsi, int))
      stopwatch()
      r=super(DBNamespaced, self).set(ids, data, allowMerge=allowMerge, existChecked=(isExist, props), onlyIfExist=onlyIfExist, strictMode=strictMode, **kwargs)
      # инкрементим `maxIndex` после добавления, чтобы в случае ошибки не увеличивать счетчик
      if r is not False and needReplaceMaxIndex:
         nsoNow['maxIndex']=max(nsoNow['maxIndex'], nsi)
      return r
