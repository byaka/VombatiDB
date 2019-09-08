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
   return DBLazyIndex, ('LazyIndex',)

class LazyChilds(DictInterface):
   __slots__ = ('__store', '__cb', '__cb_data', '__auto_lazy', '__is_node')
   def __init__(self, mapping=(), is_node=True, cb=None, cb_data=None, auto_lazy=False, **kwargs):
      self.workspace.log(2, 'Extension `LazyIndex` not effective and deprecated!')
      #? это вызывает принудительное копирование `data`, возможно лучше сделать это опциональным например для вызовов из `__setitem__`
      self.__store=dict(mapping, **kwargs)
      self.__cb=None if not callable(cb) else cb
      self.__cb_data=cb_data
      self.__auto_lazy=auto_lazy
      self.__is_node=is_node

   def __copy__(self):
      return self.__store.copy()

   def __cb_props(self, v, _1, _2, k):
      _props, _node=self.get(k, (None, None))
      if _props is v or (_props is None and not v): return
      props, node=v, _node
      self.__store[k]=(props, node)

   def __cb_node(self, v, _1, _2, k):
      _props, _node=self.get(k, (None, None))
      if _node is v or (_node is None and not v): return
      props, node=_props, v
      self.__store[k]=(props, node)

   def __iter__(self):
      return iter(self.__store)

   def __len__(self):
      return len(self.__store)

   def __getitem__(self, k):
      v=self.__store[k]
      if self.__is_node:
         auto_lazy=self.__auto_lazy
         props, node=v
         if props is None:
            props=LazyChilds(is_node=False, auto_lazy=auto_lazy, cb=self.__cb_props, cb_data=k)
         if node is None:
            node=LazyChilds(is_node=True, auto_lazy=auto_lazy, cb=self.__cb_node, cb_data=k)
         return props, node
      else:
         return v

   def __setitem__(self, k, v):
      if self.__is_node:
         props, node=v
         auto_lazy=self.__auto_lazy
         if props:
            if not isinstance(props, LazyChilds):
               props=LazyChilds(props, is_node=False, auto_lazy=auto_lazy, cb=auto_lazy and self.__cb_props, cb_data=k)
         else: props=None
         if node:
            if not isinstance(node, LazyChilds):
               node=LazyChilds(node, is_node=True, auto_lazy=auto_lazy, cb=auto_lazy and self.__cb_node, cb_data=k)
         else: node=None
         v=(props, node)
      self.__store[k]=v
      if self.__cb is not None:
         self.__cb(self, k, v, self.__cb_data)
         if not self.__auto_lazy: self.__cb=None

   def __delitem__(self, k):
      del self.__store[k]
      if self.__cb is not None:
         self.__cb(self, k, None, self.__cb_data)
         if not self.__auto_lazy: self.__cb=None

   def __contains__(self, k):
      return k in self.__store

   def __repr__(self):
      return '{0}({1})'.format(type(self).__name__, repr(self.__store))

class LazyChildsAuto(DictInterface):
   def __init__(self, mapping=(), cb=None, **kwargs):
      super(LazyChildsAuto, self).__init__(mapping=(), auto_lazy=True, cb=None, **kwargs)

class DBLazyIndex(DBBase):
   def _init(self, *args, **kwargs):
      res=super(DBLazyIndex, self)._init(*args, **kwargs)
      #! добавить конфигурирование `auto_lazy` для класса (или выбор между `LazyChildsAuto` и `LazyChilds`)
      self.___indexNodeClass=LazyChilds
      self.supports.lazyIndex=True
      return res
