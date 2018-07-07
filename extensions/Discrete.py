# -*- coding: utf-8 -*-
from ..utils import *
from ..DBBase import DBBase

def __init():
   return DBDiscrete, ('Discrete', 'Discrets')

class DBDiscrete(DBBase):
   __depend=['DBNamespaced', 'DBWithColumns']

   def _init(self, *args, **kwargs):
      res=super(DBDiscrete, self)._init(*args, **kwargs)
      if not self.supports.get('prop_branchModified', False):
         self._regProp('branchModified', bubble=True, default=True, needed=True)
         self.supports.prop_branchModified=True
      self.supports.discrete=True
      # self.settings.ns_config_keyMap.insert(2, ('columns', False))
      # self.settings.columns_default_allowUnknown=True
      # self.settings.columns_default_allowMissed=True
      # self.settings.columns_checkOnNSHook_allowed=True
      return res

   #~ остальные случаи (вроде загрузки из файла) покроются засчет выставленных атрибутов `default` и `needed`
   def _set(self, items, **kwargs):
      changes=super(DBDiscrete, self)._set(items, **kwargs)
      for ids, (isExist, data, allowMerge, props, propsUpdate) in items:
         # пропускаем специальные случаи, для них достаточно атрибутов `default` и `needed`
         if not isExist or propsUpdate: continue
         if ids not in changes or not changes[ids]: continue
         propsUpdate['branchModified']=True
      return changes

   # def _validateOnSet(self, ids, data, propsUpdate=None, **kwargs):
   #    propsUpdate['branchModified']=True
   #    return super(DBDiscrete, self)._validateOnSet(ids, data, propsUpdate=propsUpdate, **kwargs)

   # def _get(self, ids, props, **kwargs):
   #    ns, index=db_parseId2NS(ids[-1])

   def _loadedNS(self, data):
      self.__ns=data
   #    for ns, nso in data.iteritems():
   #       if 'columns' not in nso: continue
   #       columnsRaw=nso['columns']
   #       if isString(columnsRaw):
   #          columnsRaw=pickle.loads(str(columnsRaw))
   #       self.setColumns(ns, columnsRaw)
      return super(DBDiscrete, self)._loadedNS(data)

   # def setColumns(self, ns, columns):
   #    pass

   # def _namespaceChanged(self, name, setts, old):
   #    if setts is None: pass  # removed
   #    elif 'columns' not in setts or not setts['columns']:
   #       self.setColumns(name, None)
   #    elif isDict(setts['columns']):
   #       self.setColumns(name, setts['columns'])
   #    super(DBDiscrete, self)._namespaceChanged(name, setts, old)

   # def _checkIdsNS(self, ids, props=None, **kwargs):
   #    idsMap=super(DBDiscrete, self)._checkIdsNS(ids, props=props, **kwargs)
   #    if idsMap and self._settings['columns_checkOnNSHook_allowed']:
   #       stopwatch=self.stopwatch('_checkIdsNS@DBDiscrete')
   #       idNow, nsNow, nsi, nsoNow=idsMap[-1]
   #       if nsoNow and 'columns' in nsoNow:
   #          data=self.get(ids, existChecked=props, returnRaw=True, strictMode=False)
   #          if isDict(data):
   #             self._checkDataColumns(nsNow, nsoNow, ids, data, allowMerge=False)
   #       stopwatch()
   #    return idsMap
