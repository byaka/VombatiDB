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
      self.settings.columns_allowRawType=False
      return res

   def _loadedNS(self, data):
      self.__ns=data  # создаем приватную копию для текущего класса, поскольку настоящая привязана к другому
      self.__columns={}
      self._loadedColumns(self.__columns)
      for ns, nso in data.iteritems():
         if 'columns' not in nso: continue
         columnsRaw=nso['columns']
         if isString(columnsRaw):
            columnsRaw=pickle.loads(str(columnsRaw))
         self.setColumns(ns, columnsRaw)
      return super(DBWithColumns, self)._loadedNS(data)

   def _loadedColumns(self, data):
      pass

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
         else:
            colNeeded=set([colNeeded]) if isString(colNeeded) else set(colNeeded)
      else: colNeeded=False
      colDenied=set()
      colTypes={}
      colStore={}  # this can be used by another extesions
      self.__columns[ns]=(False if allowUnknown else colAllowed, colNeeded, colDenied, colTypes, colStore)
      # converting and checking column's types
      for k, o in columns.iteritems():
         if not isString(k):
            raise ValueError('Column name must be string: %s'%(k,))
         o=self._parseColumnType(ns, k, o, colStore)
         if o is False:
            # virtual column, it cant be modified
            colDenied.add(k)
            if k in colNeeded: colNeeded.remove(k)
            if k in colAllowed: colAllowed.remove(k)
            continue
         colTypes[k]=o

   def _parseColumnType(self, ns, col, vals, colStore):
      res=()
      vals=vals if isinstance(vals, (list, tuple)) else (vals,)
      for val in vals:
         r=self._convColumnType(ns, col, val, colStore)
         if not isinstance(r, tuple):
            if isinstance(r, Exception): raise r
            elif r is False:
               return False  # this column is virtual
         else:
            res+=r
      return res

   def _convColumnType(self, ns, col, val, colStore):
      res=ValueError("Unknown type for column '%s': %r"%(col, val))
      val=val.lower() if isString(val) else val
      if not isString(val):  # passed raw type
         if self._settings['columns_allowRawType']:
            self.workspace.log(2, "Raw type passed to column '%s', this can cause problems on serialization"%k)
            res=(val,)
         else:
            raise ValueError("Raw type for column '%s' not allowed"%k)
      elif val in ('num', 'number'): res=(int, float, long, complex, decimal.Decimal)  #! сюда попадут bool
      elif val in ('int', 'integer'): res=(int, long)
      elif val in ('float', 'decimal'): res=(float, decimal.Decimal)
      elif val in ('str', 'string'): res=(str, unicode)
      elif val in ('bool',): res=(bool,)
      elif val in ('list', 'array'): res=(list,)
      elif val in ('dict'): res=(dict,)
      elif val in ('any', 'obj', 'object', '*'): res=(object,)
      elif val in ('set'): res=(set,)
      elif val in ('datetime'): res=(datetime.datetime,)
      elif val in ('date'): res=(datetime.date,)
      elif val in ('time'): res=(datetime.time,)
      elif val in ('none'): res=(types.NoneType,)
      return res

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
      colAllowed, colNeeded, colDenied, colTypes, _=self.__columns[nsNow]
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
      if colNeeded and (not allowMerge or not self._findInIndex(ids, strictMode=True, calcProperties=False, skipLinkChecking=True)[0]):
         # при allowMerge и если ячейка уже существует - можно не проверять, ведь колонки уже заданы и были проверены ранее
         if dataKeys is None: dataKeys=set(data)
         stopwatch1=self.stopwatch('_checkDataColumns.colNeeded@DBWithColumns')
         tArr1=colNeeded-dataKeys
         stopwatch1()
         if tArr1:
            tArr1=', '.join('"%s"'%s for s in tArr1)
            stopwatch()
            raise ColumnError('Missed columns %s for %s'%(tArr1, tuple(idForErr)))
      if colTypes:
         stopwatch1=self.stopwatch('_checkDataColumns.colTypes@DBWithColumns')
         for k, types in colTypes.iteritems():
            if k not in data: continue
            if isinstance(data[k], types): continue
            stopwatch1()
            stopwatch()
            raise ColumnError('Value of column "%s" for %s has incorrect type, must be %s'%(k, tuple(idForErr), ' or '.join(getattr(s, '__name__', str(s)) for s in types)))
         stopwatch1()
      stopwatch()

   def _validateOnSetNS(self, ids, data, lastId, nsPrev, nsoPrev, nsMap, propsUpdate=None, isExist=None, props=None, **kwargs):
      nsNow, nsi, nsoNow=super(DBWithColumns, self)._validateOnSetNS(ids, data, lastId, nsPrev, nsoPrev, nsMap, propsUpdate=propsUpdate, **kwargs)
      stopwatch=self.stopwatch('_validateOnSetNS@DBWithColumns')
      if data is True and propsUpdate and 'link' in propsUpdate and propsUpdate['link']:
         # on link changes, we need to validate columns
         if not isExist or 'link' not in props or props['link']!=propsUpdate['link']:
            data=self.get(propsUpdate['link'], returnRaw=True, strictMode=True)
      if nsoNow and isinstance(data, dict) and 'columns' in nsoNow:
         allowMerge=kwargs['allowMerge']  # мы не используем именованный аргумент чтобы дать возможность другим расширениям переопределеить дефолтное значение этого параметра
         self._checkDataColumns(nsNow, nsoNow, ids, data, allowMerge=allowMerge)
      stopwatch()
      return nsNow, nsi, nsoNow
