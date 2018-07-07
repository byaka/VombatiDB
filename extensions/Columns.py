# -*- coding: utf-8 -*-
from ..utils import *
from ..DBBase import DBBase

def __init():
   return DBWithColumns, ('Columns', 'cols')

class ColumnError(BaseDBError):
   """ Incorrect columns. """

class DBWithColumns(DBBase):
   __depend=['DBNamespaced']

   def _init(self, *args, **kwargs):
      res=super(DBWithColumns, self)._init(*args, **kwargs)
      self.supports.columns=True
      self.settings.ns_config_keyMap.insert(2, ('columns', False))
      self.settings.columns_default_allowUnknown=True
      self.settings.columns_default_allowMissed=True
      self.settings.columns_checkOnNSHook_allowed=True
      return res

   def _loadedNS(self, data):
      self.__ns=data  # создаем приватную копию для текущего класса, поскольку настоящая привязана к другому
      self.__columns={}
      for ns, nso in data.iteritems():
         if 'columns' not in nso: continue
         columnsRaw=nso['columns']
         if isString(columnsRaw):
            columnsRaw=pickle.loads(str(columnsRaw))
         self.setColumns(ns, columnsRaw)
      return super(DBWithColumns, self)._loadedNS(data)

   def setColumns(self, ns, columns):
      if ns not in self.__ns:
         raise ValueError('Namespace "%s" not exist'%ns)
      if not columns:
         if 'columns' in self.__ns[ns]: del self.__ns[ns]['columns']
         if ns in self.__columns: del self.__columns[ns]
         return
      if not isinstance(columns, dict):
         raise ValueError('Incorrect `columns` format')
      self.__ns[ns]['columns']=columns.copy()
      allowUnknown=columns.pop('__allowUnknown', None)
      if allowUnknown is None:
         allowUnknown=self._settings['columns_default_allowUnknown']
         self.__ns[ns]['columns']['__allowUnknown']=allowUnknown
      colNeeded=columns.pop('__needed', None)
      if colNeeded is None:
         colNeeded=not(self._settings['columns_default_allowMissed'])
         self.__ns[ns]['columns']['__needed']=colNeeded
      # parsing column's config
      colAllowed=set(columns)
      if colNeeded:
         if colNeeded is True: colNeeded=colAllowed
         else: colNeeded=set([colNeeded]) if isString(colNeeded) else set(colNeeded)
      else: colNeeded=False
      colDenied=set()
      colTypes={}
      self.__columns[ns]=(False if allowUnknown else colAllowed, colNeeded, colDenied, colTypes)
      # converting and checking column's types
      for k, o in columns.iteritems():
         if not isString(k):
            raise ValueError('Column name must be string: %s'%(k,))
         if o is False:
            # virtual column, it cant be modified
            colDenied.add(k)
            if k in colNeeded: colNeeded.remove(k)
            if k in colAllowed: colAllowed.remove(k)
            continue
         elif o is True: continue  # any type
         elif o is None:
            raise NotImplementedError
         elif isFunction(o):
            raise NotImplementedError
         else:
            tArr1=o if isList(o) or isTuple(o) else (o,)
            o=()
            for oo in tArr1:
               if not isString(oo):  # passed raw type
                  self.workspace.log(2, "Raw type passed to column '%s', this can cause problems on serialization"%k)
                  oo=(oo,)
               elif oo in ('num', 'number'): oo=(int, float, long, complex, decimal.Decimal)  #! сюда попадут bool
               elif oo in ('int', 'integer'): oo=(int, long)
               elif oo in ('float',): oo=(float, decimal.Decimal)
               elif oo in ('str', 'string'): oo=(str, unicode)
               elif oo in ('bool',): oo=(bool,)
               elif oo in ('list', 'array'): oo=(list,)
               elif oo in ('dict', 'obj', 'object'): oo=(dict,)
               else:
                  raise ValueError("Unknown type for column '%s': %r"%(k, oo))
               o+=oo
         colTypes[k]=o

   def _namespaceChanged(self, name, setts, old):
      if setts is None: pass  # removed
      elif 'columns' not in setts or not setts['columns']:
         self.setColumns(name, None)
      elif isinstance(setts['columns'], dict):
         self.setColumns(name, setts['columns'])
      super(DBWithColumns, self)._namespaceChanged(name, setts, old)

   def _checkIdsNS(self, ids, props=None, **kwargs):
      idsMap=super(DBWithColumns, self)._checkIdsNS(ids, props=props, **kwargs)
      if idsMap and self._settings['columns_checkOnNSHook_allowed']:
         stopwatch=self.stopwatch('_checkIdsNS@DBWithColumns')
         idNow, nsNow, nsi, nsoNow=idsMap[-1]
         if nsoNow and 'columns' in nsoNow:
            data=self.get(ids, existChecked=props, returnRaw=True, strictMode=False)
            if isinstance(data, dict):
               self._checkDataColumns(nsNow, nsoNow, ids, data, allowMerge=False)
         stopwatch()
      return idsMap

   def _checkDataColumns(self, nsNow, nsoNow, ids, data, allowMerge):
      stopwatch=self.stopwatch('_checkDataColumns@DBWithColumns')
      colAllowed, colNeeded, colDenied, colTypes=self.__columns[nsNow]
      dataKeys=None
      idForErr=ids or 'NS(%s)'%nsNow
      if colAllowed:
         if dataKeys is None: dataKeys=set(data)
         stopwatch1=self.stopwatch('_checkDataColumns.colAllowed@DBWithColumns')
         tArr1=dataKeys-colAllowed
         stopwatch1()
         if tArr1:
            tArr1=', '.join('"%s"'%s for s in tArr1)
            tArr2=', '.join('"%s"'%s for s in colAllowed)
            stopwatch()
            raise ColumnError('Unknown columns %s for %s, allowed only %s'%(tArr1, tuple(idForErr), tArr2))
      if colDenied:
         if dataKeys is None: dataKeys=set(data)
         stopwatch1=self.stopwatch('_checkDataColumns.colDenied@DBWithColumns')
         tArr1=dataKeys&colDenied
         stopwatch1()
         if tArr1:
            tArr1=', '.join('"%s"'%s for s in tArr1)
            stopwatch()
            raise ColumnError('Disallowed columns %s for %s'%(tArr1, tuple(idForErr)))
      if colNeeded and (not allowMerge or not self._findInIndex(ids, strictMode=True, calcProperties=False)[0]):
         # при allowMerge и если ячейка уже существует - можно не проверять, ведь колонки уже заданы и были проверены ранее
         if dataKeys is None: dataKeys=set(data)
         stopwatch1=self.stopwatch('_checkDataColumns.colNeeded@DBWithColumns')
         tArr1=colNeeded-dataKeys
         stopwatch1()
         if tArr1:
            tArr1=', '.join('"%s"'%s for s in tArr1)
            stopwatch()
            raise ColumnError('Missed columns %s for %s'%(tArr1, tuple(idForErr)))
      stopwatch1=self.stopwatch('_checkDataColumns.colTypes@DBWithColumns')
      for k, types in colTypes.iteritems():
         if k not in data: continue
         if isinstance(data[k], types): continue
         stopwatch1()
         stopwatch()
         raise ColumnError('Value of column "%s" for %s has incorrect type, must be %s'%(k, tuple(idForErr), ' or '.join(getattr(s, '__name__', str(s)) for s in types)))
      stopwatch1()
      stopwatch()

   def _validateOnSetNS(self, ids, data, lastId, nsPrev, nsoPrev, nsMap, propsUpdate=None, **kwargs):
      nsNow, nsi, nsoNow=super(DBWithColumns, self)._validateOnSetNS(ids, data, lastId, nsPrev, nsoPrev, nsMap, propsUpdate=propsUpdate, **kwargs)
      stopwatch=self.stopwatch('_validateOnSetNS@DBWithColumns')
      if data is True and propsUpdate and 'link' in propsUpdate and propsUpdate['link']:
         if kwargs['existChecked'] is None:
            isExist, props, _=self._findInIndex(ids, strictMode=True)
            kwargs['existChecked']=(isExist, props)
         else:
            isExist, props=existChecked if isinstance(existChecked, tuple) else (True, existChecked)
         # on link changes, we need to validate columns
         if not isExist or 'link' not in props or props['link']!=propsUpdate['link']:
            data=self.get(propsUpdate['link'], returnRaw=True, strictMode=True)
      if nsoNow and isinstance(data, dict) and 'columns' in nsoNow:
         allowMerge=kwargs['allowMerge']
         self._checkDataColumns(nsNow, nsoNow, ids, data, allowMerge=allowMerge)
      stopwatch()
      return nsNow, nsi, nsoNow
