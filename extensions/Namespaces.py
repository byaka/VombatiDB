# -*- coding: utf-8 -*-
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
      self._idBadPattern.add('?')  #! вообще здесь запрещается использовать его только первым символом, но нужна поддержка регулярных выражений или расширенная проверка с поддержкой `startswith`
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
      self._loadedNS(self.__ns)

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
      nsMap=self.__ns
      if not nsMap: return
      if calcMaxIndex and calcMaxIndex is not True:
         calcMaxIndex=calcMaxIndex if isList(calcMaxIndex) else (list(calcMaxIndex) if isTuple(calcMaxIndex) else [calcMaxIndex])
         calcMaxIndex=[ns for ns in calcMaxIndex if ns in nsMap]
      for ids, (props, l) in self.iterIndex(ids=None, recursive=True, treeMode=True, safeMode=True, offsetLast=False, calcProperties=True):
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
               except Exception:
                  if nso['onlyIndexed']:
                     self.workspace.log(2, 'Incorrect index for id (%s%s)'%(ns, nsi))
                  continue
               nsiMax=max(nsiMax, nsi)
            nso['maxIndex']=nsiMax

   def _parseId2NS(self, id):
      stopwatch=self.stopwatch('_parseId2NS@DBNamespaced')
      res=self.namespace_pattern_parse.search(id)
      if res is None:
         if self._settings['ns_parseId_allowOnlyName'] and self.namespace_pattern_check.match(id) is not None:
            res=(id, None)
         else:
            res=(None, None)
      else:
         res=(res.group(1), res.group(2))
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

   def _namespaceChanged(self, name, setts, old):
      pass

   def configureNS(self, config, andClear=True, keyMap=None):
      if andClear: self.__ns.clear()
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
      oldSetts={}
      if ns in nsMap:
         oldSetts=nsMap[ns]
      setts={'parent':parent, 'child':child, 'onlyIndexed':onlyIndexed, 'maxIndex':oldSetts.get('maxIndex', 0)}
      setts.update(kwargs)  # позволяет хранить в namespace дополнительные данные
      if not oldSetts or oldSetts!=setts:
         nsMap[ns]=setts
         self._namespaceChanged(ns, setts, oldSetts)
         if allowCheckIndex and self._settings['ns_checkIndexOnUpdateNS']: self._checkIndexForNS(calcMaxIndex=ns)

   def delNS(self, ns, strictMode=True, allowCheckIndex=True):
      if not isString(ns) or not self.namespace_pattern_check.match(ns):
         raise ValueError('Incorrect format for NS name: "%s"'%(ns,))
      nsMap=self.__ns
      if ns not in nsMap:
         if strictMode:
            raise StrictModeError('Namespace "%s" not exist'%ns)
         return
      setts=nsMap.pop(ns)
      self._namespaceChanged(ns, None, setts)
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
            try: nsi=int(nsi)
            except Exception:
               if nsoNow['onlyIndexed']:
                  stopwatch()
                  raise NamespaceIndexError('index required for NS(%s)'%(nsNow,))
            #! поскольку у нас нет отдельного метода для добавления и отдельного для редактирования, проверка на индексы бесполезна
            # if nsi>(nsoNow['maxIndex'] or 0):
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

   def set(self, ids, data, allowMerge=True, existChecked=None, **kwargs):
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
      nsPrev=self._parseId2NS(ids[-2])[0] if len(ids)>1 else None
      nsoPrev=nsMap[nsPrev] if (nsPrev and nsPrev in nsMap) else None
      # namespace-rules checking
      nsNow, nsi, nsoNow=self._validateOnSetNS(ids, data, lastId, nsPrev, nsoPrev, nsMap, isExist=isExist, props=props, allowMerge=allowMerge, **kwargs)
      if 'backlink' in props and props['backlink']:
         # also checking namespace-rules for backlinks
         tQueue=deque(props['backlink'])
         while tQueue:
            ids2=tQueue.pop()
            isExist2, props2, _=self._findInIndex(ids2, strictMode=True, calcProperties=False, passLinkChecking=True)
            nsPrev2=self._parseId2NS(ids2[-2])[0] if len(ids2)>1 else None
            nsoPrev2=nsMap[nsPrev2] if (nsPrev2 and nsPrev2 in nsMap) else None
            self._validateOnSetNS(ids2, data, ids2[-1], nsPrev2, nsoPrev2, nsMap, isExist=isExist2, props=props2, allowMerge=allowMerge, **kwargs)
            if 'backlink' in props2 and props2['backlink']:
               tQueue.extend(props2['backlink'])
      # all checkings passed
      ids=tuple(ids)
      needReplaceMaxIndex=(nsoNow and nsi and data is not None and data is not False)
      stopwatch()
      res=super(DBNamespaced, self).set(ids, data, allowMerge=allowMerge, existChecked=(isExist, props), **kwargs)
      # инкрементим `maxIndex` после добавления, чтобы в случае ошибки не увеличивать счетчик
      if needReplaceMaxIndex:
         nsoNow['maxIndex']=max(nsoNow['maxIndex'], nsi)
      return res
