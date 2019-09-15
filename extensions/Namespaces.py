# -*- coding: utf-8 -*-
__ver_major__ = 0
__ver_minor__ = 3
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

from ..utils import *
from ..DBBase import DBBase

def __init():
   return DBNamespaced, ('Namespaces', 'NS')

class NamespaceError(BaseDBErrorPrefixed):
   """Namespace-related error"""

class NamespaceOrderError(NamespaceError):
   """Incorrect namespaces order"""

class NamespaceUnknownError(NamespaceError):
   """Unknown namespace"""

class NamespaceGenerateIdError(NamespaceError):
   """Cant generate ID for namespace"""

class NamespaceIndexError(NamespaceError):
   """Incorrect index"""

class DBNamespaced(DBBase):
   def _init(self, *args, **kwargs):
      res=super(DBNamespaced, self)._init(*args, **kwargs)
      self._idBadPattern.add(BadPatternEnds('?'))
      self._idBadPattern.add(BadPatternEnds('+'))
      self.supports.namespaces=True
      self.settings.ns_validateOnDataUpdate=False
      self.settings.ns_validateOnDataRemove=False
      self.settings.ns_checkIndexOnUpdateNS=True
      self.settings.ns_checkIndexOnConnect=True
      self.settings.ns_config_keyMap=['parent', 'child', ('onlyIndexed', True), ('onlyNumerable', False), ('localAutoIncrement', None)]
      self.settings.ns_parseId_allowOnlyName=True
      self.settings.ns_default_allowLocalAutoIncrement=False
      self.settings.ns_localAutoIncrement_reservation=False
      self.settings.ns_globalAutoIncrement_reservation=False
      self.namespace_pattern_clean_delimeter=re.compile(r'^[\-\.\s#$@]{1}')
      self.namespace_pattern_parse=re.compile(r'^([a-zA-Z_]+)(\d+|[\-\.\s#$@].+)$')
      self.namespace_pattern_check=re.compile(r'^[a-zA-Z_]+$')

      self._regProp('localAutoIncrement', default=None, inherit=False, needed=False, bubble=False, persistent=False)
      self.supports.prop_localAutoIncrement=True
      self.supports.generateIdRandom=False
      self.supports.generateIdAI_local='Use `NS+` instead of id, forexample `("a1", "b1", "c+")`.'
      self.supports.generateIdAI_global='Use `NS?` instead of id, forexample `("a1", "b1", "c?")`.'
      self.__LAInc_enabled=[False, set()]  # при изменении NS вычисляется, разрешено ли хоть гдето использование LAInc, а также записывается какие NS их разрешили
      return res

   def _initNS(self, data=None):
      if '_namespace' not in data: data['_namespace']={}
      self.__ns=data['_namespace']
      self._repareNSConfig()
      #! хранение дефолтных значений внутри конфига не позволяет запустить туже базу с другим набором дефолтных параметров
      self._loadedNS(self.__ns)

   def _repareNSConfig(self, keyMap=None):
      keyMap=keyMap or self._settings['ns_config_keyMap']
      NEEDED=object()
      for ns, nso in self.__ns.iteritems():
         for k in keyMap:
            if isString(k):
               vdef=NEEDED
            elif isTuple(k) and len(k)==2:
               k, vdef=k
            else:
               raise ValueError('Unknown config for Namespaces: %s'%k)
            if k in nso: continue
            if vdef is NEEDED:
               raise KeyError('Missed required config-key for NS(%s)'%ns)
            nso[k]=vdef

   def _connect(self, **kwargs):
      super(DBNamespaced, self)._connect(**kwargs)
      if self._settings['ns_checkIndexOnConnect']:
         if not self.__ns:
            self.workspace.log(2, 'Namespaces not configured!')
         else:
            self.__calcLAInc()
            self._checkIndexForNS(calcGlobalMaxIndex=True, calcLocalMaxIndex=True)

   def _loadedNS(self, data):
      pass

   def _loadedMeta(self, data):
      self._initNS(data)
      return super(DBNamespaced, self)._loadedMeta(data)

   def configureNS(self, config, andClear=True, keyMap=None):
      if andClear:
         old=self.__ns.copy()
         self.__ns.clear()
         self.__LAInc_enabled=[False, set()]
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
         self._checkIndexForNS(calcGlobalMaxIndex=(True if andClear else tArr1), calcLocalMaxIndex=(True if andClear else tArr1))

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
      for k in self._settings['ns_config_keyMap']:
         if isTuple(k) and len(k)==2:
            k, vdef=k
            if k not in nsoNow:
               nsoNow[k]=vdef
      #
      if nsoNow['localAutoIncrement'] is not None and nsoNow['localAutoIncrement'] is not True and nsoNow['localAutoIncrement'] is not False and isinstance(nsoNow['localAutoIncrement'], (list, tuple, dict, types.GeneratorType)):
         nsoNow['localAutoIncrement']=set(nsoNow['localAutoIncrement'])
      if isinstance(nsoNow['localAutoIncrement'], set) and not nsoNow['localAutoIncrement']:
         nsoNow['localAutoIncrement']=False
      #
      if not nsoOld or nsoOld!=nsoNow:
         nsMap[ns]=nsoNow
         self._namespaceChanged(ns, nsoNow, nsoOld)
         if allowCheckIndex and self._settings['ns_checkIndexOnUpdateNS']:
            self._checkIndexForNS(calcGlobalMaxIndex=ns, calcLocalMaxIndex=ns)
      self.__calcLAInc(onlyFor=ns)

   def delNS(self, ns, strictMode=True, allowCheckIndex=True):
      if not isString(ns) or not self.namespace_pattern_check.match(ns):
         raise ValueError('Incorrect format for NS name: "%s"'%(ns,))
      nsMap=self.__ns
      if ns not in nsMap:
         if strictMode:
            raise StrictModeError('Namespace "%s" not exist'%ns)
         return
      nso=nsMap.pop(ns)
      self.__calcLAInc(onlyFor=ns)
      self._namespaceChanged(ns, None, nso)
      if allowCheckIndex and self._settings['ns_checkIndexOnUpdateNS']: self._checkIndexForNS(calcGlobalMaxIndex=False)

   def __calcLAInc(self, onlyFor=False):
      if not self.settings_frozen: return
      __LAInc_def=self._settings['ns_default_allowLocalAutoIncrement']
      if __LAInc_def is not False and __LAInc_def is not True and (not __LAInc_def or not isinstance(__LAInc_def, set)):
         self.settings._MagicDictCold__unfreeze()
         __LAInc_def=self.settings['ns_default_allowLocalAutoIncrement']=__LAInc_def or False
         if __LAInc_def is not True and not isinstance(__LAInc_def, set):
            __LAInc_def=self.settings['ns_default_allowLocalAutoIncrement']=set(__LAInc_def)
         self.settings._MagicDictCold__freeze()
      nsMap=self.__ns
      if onlyFor is False:
         onlyFor=nsMap
      elif isinstance(onlyFor, (str, unicode)):
         onlyFor=(onlyFor,)
      else:
         onlyFor=[ns for ns in onlyFor]
      __LAInc_enabled=self.__LAInc_enabled
      if __LAInc_def is not False: __LAInc_enabled[0]=True
      for ns in onlyFor:
         if ns not in nsMap:
            if __LAInc_enabled[0] and ns in __LAInc_enabled[1]:
               __LAInc_enabled[1].remove(ns)
               if not __LAInc_enabled[1] and __LAInc_def is False:
                  __LAInc_enabled[0]=False
            continue
         nso=nsMap[ns]
         _LAInc=nso['localAutoIncrement']
         if _LAInc is None:
            _LAInc=nso['localAutoIncrement']=__LAInc_def
         if isinstance(_LAInc, set) or _LAInc is True:
            __LAInc_enabled[1].add(ns)
            __LAInc_enabled[0]=True
         elif __LAInc_enabled[0] is True and ns in __LAInc_enabled[1]:
            __LAInc_enabled[1].remove(ns)
            if not __LAInc_enabled[1] and __LAInc_def is False:
               __LAInc_enabled[0]=False

   def _checkIdsNS(self, ids, nsIndexMap=None, **kwargs):
      stopwatch=self.stopwatch('_checkIdsNS@DBNamespaced')
      nsMap=self.__ns
      if not nsMap: return None
      tArr=[]
      nsPrev, nsoPrev=None, None
      for idNow in ids:
         nsNow, nsiNow=self._parseId2NS(idNow)
         numerable_nsiNow=False
         if (nsoPrev and nsoPrev['child'] and (
            (nsoNow and nsNow not in nsoPrev['child']) or
            (not nsoNow and None not in nsoPrev['child']))):
            stopwatch()
            raise NamespaceOrderError('NS(%s) cant be child of NS(%s)'%(nsNow, nsPrev))
         nsoNow=None
         if nsNow and nsNow in nsMap:
            nsoNow=nsMap[nsNow]
            if nsiNow is None and nsoNow['onlyIndexed']:
               raise NamespaceIndexError('index required for NS(%s)'%(nsNow,))
            if nsiNow is not None:
               try:
                  if isinstance(nsiNow, (str, unicode)):
                     nsiNow=int(self.namespace_pattern_clean_delimeter.sub('', nsiNow))
                  else:
                     nsiNow=int(nsiNow)
                  numerable_nsiNow=True
               except (ValueError, TypeError):
                  if nsoNow['onlyNumerable']:
                     stopwatch()
                     raise NamespaceIndexError('numerable index required for NS(%s)'%(nsNow,))
            if nsIndexMap is not None:
               if nsNow in nsIndexMap:
                  nsIndexMap[nsNow].append(nsiNow)
               else:
                  nsIndexMap[nsNow]=[nsiNow]
            if nsoNow['parent'] and (
               (nsoPrev and nsPrev not in nsoNow['parent']) or
               (not nsoPrev and None not in nsoNow['parent'])):
               stopwatch()
               raise NamespaceOrderError('NS(%s) cant be parent of NS(%s)'%(nsPrev, nsNow))
         nsPrev, nsoPrev=nsNow, nsoNow
         tArr.append((idNow, nsNow, nsiNow, nsoNow, numerable_nsiNow))
      stopwatch()
      return tArr

   def _checkIndexForNS(self, calcGlobalMaxIndex=True, calcLocalMaxIndex=False):
      #! разным расширениям зачастую нужен механизм обхода всех обьектов при инициализации. нет смысла делать это раздельно, нужен единый механизм
      #? однако этаже функция используется при реконфигурации неймспейсов для выполнения валидации.
      stopwatch=self.stopwatch('_checkIndexForNS@DBNamespaced')
      nsMap=self.__ns
      if not nsMap: return
      __LAInc_enabled=self.__LAInc_enabled
      __LAInc_def=self.settings['ns_default_allowLocalAutoIncrement']
      #
      if calcGlobalMaxIndex is True: calcGlobalMaxIndex=nsMap.keys()
      elif calcGlobalMaxIndex:
         if isinstance(calcGlobalMaxIndex, (list, tuple, set)):
            calcGlobalMaxIndex=list(ns for ns in calcGlobalMaxIndex if ns in nsMap)
         else:
            calcGlobalMaxIndex=([calcGlobalMaxIndex] if calcGlobalMaxIndex in nsMap else ())
         if not len(calcGlobalMaxIndex): calcGlobalMaxIndex=False
      else:
         calcGlobalMaxIndex=False
      #
      if not __LAInc_enabled[0]:
         calcLocalMaxIndex=False
      elif calcLocalMaxIndex and calcLocalMaxIndex is not True:
         if isinstance(calcLocalMaxIndex, (list, tuple, set)):
            calcLocalMaxIndex=set(ns for ns in calcLocalMaxIndex if ns in nsMap)
         else:
            calcLocalMaxIndex=set([calcLocalMaxIndex] if calcLocalMaxIndex in nsMap else ())
         if not calcLocalMaxIndex:
            calcLocalMaxIndex=False
      #
      nsIndexMap={} if calcGlobalMaxIndex is not False else None
      for (idsParent, (propsParent, lParent)), (ids, (props, l)) in self.iterBranch(recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True, returnParent=True):
         idsChain=self._checkIdsNS(ids, nsIndexMap=nsIndexMap, props=props, childCount=l)
         _, ns, nsi, nso, numerable_nsiNow=idsChain[-1]
         # localAutoIncrement
         stopwatch1=self.stopwatch('_checkIndexForNS.localAutoIncrement@DBNamespaced')
         if numerable_nsiNow and (calcLocalMaxIndex is True or (calcLocalMaxIndex is not False and ns in calcLocalMaxIndex)) and idsParent and (ns in __LAInc_enabled[1] or nso is None):
            tArr=nso['localAutoIncrement'] if nso is not None else __LAInc_def
            if tArr is True or (tArr is not False and self._parseId2NS(idsParent[-1])[0] in tArr):
               tArr=None
               if 'localAutoIncrement' in propsParent:
                  tArr=propsParent['localAutoIncrement']
                  if ns in tArr and tArr[ns]>nsi: tArr=None
                  else:
                     tArr=tArr.copy()
                     tArr[ns]=nsi
               else:
                  if not propsParent: propsParent={}  #~ для совместимости с хаком ниже
                  tArr={ns:nsi}
               if tArr is not None:
                  self. _markInIndex(idsParent, strictMode=True, createNotExisted=False, localAutoIncrement=tArr)
                  propsParent['localAutoIncrement']=tArr  #~ генератор `iterBranch` не отслеживает изменение родителя в процессе работы и отдает всегда начальные данные
         stopwatch1()
      # globalAutoIncrement
      if calcGlobalMaxIndex is not False and nsIndexMap:
         stopwatch1=self.stopwatch('_checkIndexForNS.globalAutoIncrement@DBNamespaced')
         for ns in calcGlobalMaxIndex:
            if ns not in nsIndexMap: continue
            nso=nsMap[ns]
            if not nso['maxIndex']: nso['maxIndex']=0
            nso['maxIndex']=max(nso['maxIndex'], *nsIndexMap[ns])
         stopwatch1()
      stopwatch()

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

   def _generateIdNS_globalAutoIncrement(self, ns, nso, localAutoIncrement=False):
      stopwatch=self.stopwatch('_generateIdNS_globalAutoIncrement@DBNamespaced')
      if nso is None:
         raise NamespaceUnknownError('Cant generate index for unknown NS(%s)'%(ns,))
      ii=(nso['maxIndex'] or 0)+1
      if self._settings['ns_globalAutoIncrement_reservation']: nso['maxIndex']=ii
      stopwatch()
      return ii

   def _generateIdNS_localAutoIncrement(self, ns, nso, nsPrev, idsParent, propsParent):
      if not self.__LAInc_enabled[0] or (ns not in self.__LAInc_enabled[1] and ns is not None):
         raise NamespaceGenerateIdError('LocalAutoIncrement disabled for parent NS(%s) by NS(%s)'%(nsPrev, ns))
      stopwatch=self.stopwatch('_generateIdNS_localAutoIncrement@DBNamespaced')
      if nso is not None:
         tArr=nso['localAutoIncrement']
      else:
         tArr=self._settings['ns_default_allowLocalAutoIncrement']
      if tArr is not True and nsPrev not in tArr:
         raise NamespaceGenerateIdError('LocalAutoIncrement disabled for parent NS(%s) by NS(%s)'%(nsPrev, ns))
      if 'localAutoIncrement' not in propsParent:
         if self._settings['ns_localAutoIncrement_reservation']: tArr={ns:1}
         ii=1
      else:
         tArr=propsParent['localAutoIncrement']
         if not self._settings['ns_localAutoIncrement_reservation']:
            ii=1 if ns not in tArr else tArr[ns]+1
         else:
            tArr=tArr.copy()
            if ns in tArr:
               tArr[ns]+=1
               ii=tArr[ns]
            else:
               ii=tArr[ns]=1
      if self._settings['ns_localAutoIncrement_reservation']:
         self._markInIndex(idsParent, strictMode=True, createNotExisted=False, localAutoIncrement=tArr)
      if nso is not None and self._settings['ns_globalAutoIncrement_reservation']:
         nso['maxIndex']=max(nso['maxIndex'], ii)
      stopwatch()
      return ii

   def ids2ns_generator(self, ids):
      assert isinstance(ids, (tuple, list))
      for id in ids:
         ns, nsi, nso=self._parseId2NS(id, needNSO=True)
         yield (id, ns, nsi, nso)

   def ids2ns(self, ids):
      return tuple(self.ids2ns_generator(ids))

   def _namespaceChanged(self, ns, nsoNow, nsoOld):
      pass

   def _validateOnSetNS(self, ids, data, nsNow, nsiNow, nsoNow, nsPrev, nsiPrev, nsoPrev, isExist=None, lastId=None, **kwargs):
      if isExist:
         if nsoNow is not None and nsiNow is not None:
            try:
               nsiNow=int(nsiNow)
               return nsiNow, True
            except (ValueError, TypeError):
               return nsiNow, False
      stopwatch=self.stopwatch('_validateOnSetNS@DBNamespaced')
      numerable_nsiNow=False
      if nsoNow is not None:
         if nsiNow is None and nsoNow['onlyIndexed']:
            raise NamespaceIndexError('index required for NS(%s)'%(nsNow,))
         if nsiNow is not None:
            try:
               if isinstance(nsiNow, (str, unicode)):
                  nsiNow=int(self.namespace_pattern_clean_delimeter.sub('', nsiNow))
               else:
                  nsiNow=int(nsiNow)
               numerable_nsiNow=True
            except (ValueError, TypeError):
               if nsoNow['onlyNumerable']:
                  stopwatch()
                  raise NamespaceIndexError('numerable index required for NS(%s)'%(nsNow,))
         #! поскольку у нас нет отдельного метода для добавления и отдельного для редактирования, проверка на индексы бесполезна в таком виде
         # if isinstance(nsiNow, int) and nsiNow>(nsoNow['maxIndex'] or 0):
         #    stopwatch()
         #    raise NamespaceIndexError('too large value %s for NS(%s)'%(nsiNow, nsNow))
         if nsoNow['parent'] and (
            (nsoPrev is not None and nsPrev not in nsoNow['parent']) or
            (nsoPrev is None and None not in nsoNow['parent'])):
            stopwatch()
            raise NamespaceOrderError('NS(%s) cant be parent of NS(%s)'%(nsPrev, nsNow))
      if (nsoPrev is not None and nsoPrev['child'] and (
         (nsoNow is not None and nsNow not in nsoPrev['child']) or
         (nsoNow is None and None not in nsoPrev['child']))):
         stopwatch()
         raise NamespaceOrderError('NS(%s) cant be child of NS(%s)'%(nsNow, nsPrev))
      stopwatch()
      return nsiNow, numerable_nsiNow

   def set(self, ids, data, allowMerge=True, existChecked=None, onlyIfExist=None, strictMode=False, **kwargs):
      stopwatch=self.stopwatch('set@DBNamespaced')
      ids=ids if isinstance(ids, list) else(list(ids) if isinstance(ids, tuple) else [ids])  # needed for modify on autoincrement
      lastId=ids[-1]
      if lastId is None:
         raise NotSupportedError('Generating random ID disabled by *DBNamespaced* extension, use local or global auto-increment instead')
      _idsLen=len(ids)
      nsMap=self.__ns
      isExist, props=False, {}
      nsPrev, nsiPrev=NULL, None
      nsNow, nsiNow, nsoNow=NULL, None, None
      idsParent=tuple(ids[:-1])
      wasAutoGen=False
      numerable_nsiNow=False
      _isPotentialNSAI=self.namespace_pattern_check.match(lastId[:-1]) if len(lastId)>1 else False
      if _isPotentialNSAI and lastId[-1]=='?':
         # namespace-based globalAutoIncrement newId-generator
         stopwatch1=self.stopwatch('set.globalAutoIncrement@DBNamespaced')
         wasAutoGen=1
         numerable_nsiNow=True
         nsNow=lastId[:-1]
         nsoNow=nsMap.get(nsNow, None)
         nsiNow=self._generateIdNS_globalAutoIncrement(nsNow, nsoNow)
         lastId=ids[-1]='%s%i'%(nsNow, nsiNow)
         stopwatch1()
      elif _isPotentialNSAI and lastId[-1]=='+':
         # namespace-based localAutoIncrement newId-generator
         if _idsLen==1:
            raise NamespaceGenerateIdError('No parent for NS(%s)'%(ns,))
         stopwatch1=self.stopwatch('set.localAutoIncrement@DBNamespaced')
         wasAutoGen=2
         numerable_nsiNow=True
         nsPrev, nsiPrev=self._parseId2NS(ids[-2]) if _idsLen>1 else (None, None)
         _, propsParent, _, _=self._findInIndex(idsParent, strictMode=True, calcProperties=False)
         nsNow=lastId[:-1]
         nsoNow=nsMap.get(nsNow, None)
         nsiNow=self._generateIdNS_localAutoIncrement(nsNow, nsoNow, nsPrev, idsParent, propsParent)
         lastId=ids[-1]='%s%i'%(nsNow, nsiNow)
         stopwatch1()
      else:
         if existChecked is None:
            isExist, props, _, _=self._findInIndex(ids, strictMode=True, calcProperties=True, skipLinkChecking=True)
         else:
            isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
      ids=tuple(ids)
      if onlyIfExist is not None and isExist!=onlyIfExist:
         if strictMode:
            raise ExistStatusMismatchError('expected "isExist=%s" for %s'%(onlyIfExist, ids))
         return ids
      if nsPrev is NULL:
         nsPrev, nsiPrev=self._parseId2NS(ids[-2]) if _idsLen>1 else (None, None)
      nsoPrev=nsMap[nsPrev] if (nsPrev and nsPrev in nsMap) else None
      #
      if nsNow is NULL:
         nsNow, nsiNow=self._parseId2NS(lastId)
         nsoNow=nsMap.get(nsNow, None)
      # namespace-rules checking
      if not isExist or (data is None and self._settings['ns_validateOnDataRemove']) or (data is not None and self._settings['ns_validateOnDataUpdate']):
         nsiNow, numerable_nsiNow=self._validateOnSetNS(ids, data, nsNow, nsiNow, nsoNow, nsPrev, nsiPrev, nsoPrev, lastId=lastId, isExist=isExist, props=props, allowMerge=allowMerge, **kwargs)
         if 'backlink' in props and props['backlink']:
            stopwatch1=self.stopwatch('set.checkBacklinked@DBNamespaced')
            # also checking namespace-rules for backlinks
            _queue=deque(props['backlink'])
            _queuePop=_queue.pop
            _queueExtend=_queue.extend
            _findInIndex=self._findInIndex
            _parseId2NS=self._parseId2NS
            _validateOnSetNS=self._validateOnSetNS
            while _queue:
               ids2=_queuePop()
               isExist2, props2, _=_findInIndex(ids2, strictMode=True, calcProperties=True, skipLinkChecking=True)
               lastId2=ids2[-1]
               nsNow2, nsiNow2=self._parseId2NS(lastId2)
               nsoNow2=nsMap.get(nsNow2, None)
               nsPrev2, nsiPrev2=_parseId2NS(ids2[-2]) if len(ids2)>1 else (None, None)
               nsoPrev2=nsMap[nsPrev2] if (nsPrev2 and nsPrev2 in nsMap) else None
               _validateOnSetNS(ids2, data, nsNow2, nsiNow2, nsoNow2, nsPrev2, nsiPrev2, nsoPrev2, nsMap=nsMap, lastId=lastId2, isExist=isExist2, props=props2, allowMerge=allowMerge, **kwargs)
               if 'backlink' in props2 and props2['backlink']:
                  _queueExtend(props2['backlink'])
            stopwatch1()
      # all checkings passed
      needReplaceMaxIndex=numerable_nsiNow and not isExist and nsoNow and nsiNow and isinstance(nsiNow, int)  #? здесь было ` and data is not None` что отключает корректировку автоинкрементов при удалении обьекта
      stopwatch()
      r=super(DBNamespaced, self).set(ids, data, allowMerge=allowMerge, existChecked=(isExist, props), onlyIfExist=onlyIfExist, strictMode=strictMode, **kwargs)
      # инкрементим `maxIndex` после добавления, чтобы в случае ошибки не увеличивать счетчик
      if needReplaceMaxIndex:
         # globalAutoIncrement
         if nsoNow is not None and (wasAutoGen is False or not self._settings['ns_globalAutoIncrement_reservation']):
            nsoNow['maxIndex']=max(nsoNow['maxIndex'], nsiNow)
         # localAutoIncrement
         if _idsLen>1 and (wasAutoGen!=2 or not self._settings['ns_localAutoIncrement_reservation']) and self.__LAInc_enabled[0] and (nsNow in self.__LAInc_enabled[1] or nsoNow is None):
            stopwatch1=self.stopwatch('set_updateAutoIncrement.local@DBNamespaced')
            tArr=nsoNow['localAutoIncrement'] if nsoNow is not None else self._settings['ns_default_allowLocalAutoIncrement']
            if tArr is True or nsPrev in tArr:
               _, propsParent, _, _=self._findInIndex(idsParent, strictMode=True, calcProperties=False)
               if 'localAutoIncrement' not in propsParent:
                  tArr={nsNow:nsiNow}
               else:
                  tArr=propsParent['localAutoIncrement'].copy()
                  tArr[nsNow]=max(tArr[nsNow], nsiNow) if nsNow in tArr else nsiNow
               self._markInIndex(idsParent, strictMode=True, createNotExisted=False, localAutoIncrement=tArr)
            stopwatch1()
      return r
