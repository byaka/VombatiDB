# -*- coding: utf-8 -*-
__ver_major__ = 0
__ver_minor__ = 2
__ver_patch__ = 0
__ver_sub__ = "experimental"
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
   return DBDiscrete, ('Discrete', 'Discrets')

class DiscreteCalcError(BaseDBErrorPrefixed):
   """Error happened while calcing discrete column"""

class DiscreteColumn(object):
   """
   Обьект слжуит как контейнер настроек для дискретизации.

   :param func|str cb: Коллбек, выполняющий расчет значения, либо идентификатор одного из предопределенных (типа min, max, median, quantile, average, count).
   :param bool traverse: Означает, что коллбек будет вызван рекурсивно для всей ветки, таким образом осуществляя обход всех детей (с учетом того, что если ребенок имеет посчитанное значение, и не имеет всплывшего `Props@branchModified`, то его детей можно не смотреть). Если этот режим включен, колонка не может зависеть от других колонок в томже самом обьекте, только в дочерних.
   :param list|tuple|set|bool depends: Указывает, от значений каких других колонок зависит эта. `True` означает от нее самой, `False` отключает зависимости. Этот параметр позволяет реализовать автоматическое отслеживание и всплытие `Props@branchModified`.
   """

   __slots__=('cbPredefined', 'raw', 'data')
   cbPredefined={}
   def __init__(self, cb, traverse=True, depends=True):
      #! если `cb` передан в виде функции, его нельзя будет запиклить
      if isinstance(cb, (str, unicode)) and cb not in self.cbPredefined:
         raise ValueError('Unknown predefined callback "%s", allowed only: %s'%(cb, ', '.join(self.cbPredefined)))
      self.raw=(cb, traverse, depends)
      if depends is True or isinstance(depends, (str, unicode)): depends=(depends,)
      elif not depends: depends=False
      else: depends=set(depends)
      traverse=True if traverse else False
      cb=cb if cb not in self.cbPredefined else self.cbPredefined[cb]
      self.data=(cb, traverse, depends)

   #! возможно вместо предподготовки и хранения `self.data` лучше дать метод, который будет генерировать эти данные налету

   def __repr__(self):
      return '%s(cb=%s, traverse=%s, depends=%s)'%((self.__class__.__name__,)+self.raw)

   def __getstate__(self):
      return self.raw

   def __setstate__(self, raw):
      self.__init__(raw[0], traverse=raw[1], depends=raw[2])

class DBDiscrete(DBBase):
   __depend=['DBNamespaced', 'DBWithColumns']

   def _init(self, *args, **kwargs):
      res=super(DBDiscrete, self)._init(*args, **kwargs)
      if not self.supports.get('prop_branchModified', False):
         self._regProp('branchModified', bubble=True, default=True, needed=True)
         self.supports.prop_branchModified=True
      self.__allDiscreteColumnsByNS={}  # используется, чтобы конвертировать `prop@branchModified` из `True` в настоящий список колонок
      self.__tmpCache={}  #! это должно реализовываться средствами `Store*` расширения
      self.settings.ns_validateOnUpdate=True
      self.supports.discrete=True
      self.settings.discrete_cacheble=False  # for supporting caching of calculated values, we need to disable some perf-tricks
      return res

   def _connect(self, **kwargs):
      self.__tmpCache=lruDict(1000)
      return super(DBDiscrete, self)._connect(**kwargs)

   def __bubbleCB(self, k, vSelf, vPop, ids):
      if vSelf is True or vPop is True: return True
      vSelf=vSelf.copy()  #~ `поведение `dictMergeEx()` таково, что если коллбек вернет `vSelf`, то не будет вызван `changedCB` и следовательно другие расширения не узнают, что произошло изменение
      vSelf.update(vPop)
      return vSelf

   def _loadedNS(self, data):
      self.__ns=data
      return super(DBDiscrete, self)._loadedNS(data)

   def _loadedColumns(self, data):
      self.__columns=data
      return super(DBDiscrete, self)._loadedColumns(data)

   def setColumns(self, ns, columns):
      self.__allDiscreteColumnsByNS.pop(ns)
      r=super(DBDiscrete, self).setColumns(data)
      try:
         self.__allDiscreteColumnsByNS[ns]=set(self.__columns[ns][-1]['discrete'])
      except (KeyError, IndexError): pass
      return r

   def _convColumnType(self, ns, col, o, colStore):
      stopwatch=self.stopwatch('_convColumnType@DBDiscrete')
      if isinstance(o, DiscreteColumn):
         # эта колонка самогенерируется на основе обьектов из ветки - при включенном `traverse` по факту это и есть дискретизация
         if 'discrete' not in colStore:
            colStore['discrete']={}
         cb, traverse, depends=o.data
         if depends is not False:
            depends=set((col if s is True else s) for s in depends)
         colStore['discrete'][col]=(cb, traverse, depends)
         stopwatch()
         return False  # помечает, что колонка является виртуальной
      stopwatch()
      return super(DBDiscrete, self)._convColumnType(ns, col, o, colStore)

   #~ остальные случаи (вроде загрузки из файла) покроются засчет выставленных атрибутов `default` и `needed`
   #? неуверен настчет удаления обьекта - всплывет ли тогда корректно, особенно при удалении битых ссылок про чих специальных кейсах
   def _setData(self, items, **kwargs):
      changes=super(DBDiscrete, self)._setData(items, **kwargs)
      stopwatch=self.stopwatch('_setData@DBDiscrete')
      for ids, (isExist, data, allowMerge, props, propsUpdate) in items:
         if not isExist: continue
         if ids not in changes: continue
         if propsUpdate.get('branchModified') is True: continue  # already marked as `full-changed`
         whatChanged=changes[ids]
         if not whatChanged: continue
         if whatChanged is True:
            # no details about what keys changed
            propsUpdate['branchModified']=True
         else:
            currChanged=props['branchModified']
            if currChanged is False:
               currChanged=set(whatChanged)
            else:
               currChanged.update(whatChanged)
            propsUpdate['branchModified']=currChanged
      stopwatch()
      return changes

   def _getData(self, ids, props, ns=NULL, **kwargs):
      if ns is NULL:
         ns=self._parseId2NS(ids[-1])
      res=super(DBDiscrete, self)._getData(ids, props, ns=ns, **kwargs)
      ns=ns[0]
      if ns is None or ns not in self.__columns or 'discrete' not in self.__columns[ns][-1]:
         return res
      branchModified=props['branchModified']
      if branchModified is True:
         branchModified=self.__allDiscreteColumnsByNS[ns].copy()
      _discreteSetts=self.__columns[ns][-1]['discrete']
      res=res.copy()  #! по сути это должно зависеть от реализации расширения `Store*`
      need2calcInplace, need2calcTraverse=[], []
      calced2cache={}
      _tmpCache=self.__tmpCache.get(ids)
      for key, (cb, traverse, depends) in _discreteSetts.iteritems():
         if _tmpCache and key in _tmpCache:
            #! пока не определено поведение при `depends=False`
            if branchModified is False or not branchModified.intersection(depends):
               res[key]=calced2cache[key]=_tmpCache[key]
               continue
         elif traverse is True:
            need2calcTraverse.append(key)
         else:
            need2calcInplace.append(key)
         res[key]=calced2cache[key]=None
      #! сейчас не учитывается то, что от данной колонки могут зависеть другие колонки, которые в ином случае небыли бы пересчитаны. для разрешения этой ситуации нужно в `setColumns()` генерировать подробную карту зависимостей, которая позволит разрешать такие конфликты за o(1)

      #! однако это не решает другую проблему - `need2calcInplace` колонка может зависеть от `need2calcTraverse` и наоборот, и в какой последовательности выполнять эти 2 блока ниже, неясно
      #? сейчас при расчете `need2calcTraverse` в коллбек передаются только дочерние обьекты, но не он сам. если сделать это ограничение by-design (traverse-колонки не могут ссылаться на колонки в самом обьекте) это избавит от проблемы выше и вообще упростит реализацию
      #
      if need2calcTraverse:
         # обходим только прямых потомков, остальное выполнится при вызове `self.get()`
         #? такой подход фактически является рекурсивным вызовом самой себя, что плохо. но чтобы отказаться от рекурсии очевидно придется скопировать сюда логику `self.get()` а также сильно усложнить код. надо подумать, возможно есть иные варианты
         #~ `skipLinkChecking=True` здесь используется, поскольку эта проверка всеравно будет выполнена при `self.get()`
         g=self.iterBranch(ids=ids, recursive=False, treeMode=False, safeMode=False, calcProperties=False, skipLinkChecking=True)
         for ids2, (props2, l2) in g:
            res2=self.get(ids2, existChecked=(True, props2), returnRaw=True, strictMode=False)
            #! в процессе вычисления `self.get` очевидно происходит всплытие `prop@branchModified`, но кодом ниже мы перетрем изменения для текущего обьекта
            #! кроме того похоже что изза этого всплытия принудительно сбросится кеш для следующего запроса данного обьекта, несмотря на то что мы его толькочто посчитали
            for key in need2calcTraverse:
               try:
                  res[key]=calced2cache[key]=_discreteSetts[key][0](res2, res[key])
               except Exception:
                  raise DiscreteCalcError(getErrorInfo())
      #
      if need2calcInplace:
         for key in need2calcInplace:
            try:
               res[key]=calced2cache[key]=_discreteSetts[key][0](res)
            except Exception:
               raise DiscreteCalcError(getErrorInfo())
      #
      branchModified=False
      if need2calcInplace or need2calcTraverse:
         if branchModified is True or branchModified is False:
            props['branchModified']=set(need2calcInplace+need2calcTraverse)
         else:
            props['branchModified'].update(need2calcInplace, need2calcTraverse)
      if self._settings['discrete_cacheble']:
         self._markInIndex(ids, branchModified=branchModified)
      else:
         props['branchModified']=branchModified
      #
      self.__tmpCache[ids]=calced2cache
      #
      return res

   # def _validateOnSet(self, ids, data, nsNow, nsiNow, nsoNow, nsPrev, nsiPrev, nsoPrev, propsUpdate=None, **kwargs):
   #    propsUpdate['branchModified']=True
   #    super(DBDiscrete, self)._validateOnSet(ids, data, nsNow, nsiNow, nsoNow, nsPrev, nsiPrev, nsoPrev, propsUpdate=propsUpdate, **kwargs)
