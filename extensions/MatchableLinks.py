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
   return DBMatchableLinks, ('MatchableLinks',)

class DBMatchableLinks(DBBase):
   __depend=[]

   def _init(self, *args, **kwargs):
      res=super(DBMatchableLinks, self)._init(*args, **kwargs)
      self._regProp('linkedChilds', default=None, inherit=False, needed=False, bubble=False, persistent=False)
      self.supports.prop_linkedChilds=True
      self.supports.matchableLinks=True
      self.__matchMethodConvMap={
         'and':'intersection',
         'or':setsSymDifference,
         'join':'union',
      }
      self.settings.linkedChilds_default_do=True
      self.settings.linkedChilds_inheritNSFlags=False
      if self.supports.get('namespaces'):
         self.settings.ns_config_keyMap.append(('linkChilds', None))
      return res

   def _connect(self, **kwargs):
      super(DBMatchableLinks, self)._connect(**kwargs)
      self.__reparePropFromIndex()

   def __isEnabled(self, ids, doDefault=NULL):
      stopwatch=self.stopwatch('__isEnabled@DBMatchableLinks')
      if doDefault is NULL:
         doDefault=self._settings['linkedChilds_default_do']
      r=True
      if self.supports.get('namespaces'):
         _inherit=self._settings['linkedChilds_inheritNSFlags']
         _inheritCallable=_inherit and callable(_inherit)
         if _inherit:
            doLinkedChilds=True
            for _, _, _, nso in self.ids2ns_generator(ids):
               v=doDefault if nso is None or nso['linkChilds'] is None else nso['linkChilds']
               if _inheritCallable:
                  doLinkedChilds=_inherit(doLinkedChilds, v)
               else:
                  doLinkedChilds=doLinkedChilds and v
               if not doLinkedChilds: break
         else:
            _, _, nso=self._parseId2NS(ids[-1], needNSO=True)
            doLinkedChilds=doDefault if nso is None or nso['linkChilds'] is None else nso['linkChilds']
         if not doLinkedChilds: r=False
      elif not doDefault: r=False
      stopwatch()
      return r

   def __reparePropFromIndex(self):
      #! разным расширениям зачастую нужен механизм обхода всех обьектов при инициализации. нет смысла делать это раздельно, нужен единый механизм
      _parentPrev=None
      _queue=None
      for ids, (props, l) in self.iterBranch(recursive=True, treeMode=True, safeMode=False, offsetLast=False, calcProperties=False):
         if len(ids)==1:
            if _queue and _parentPrev and self.__isEnabled(_parentPrev):
               self._markInIndex(_parentPrev, strictMode=True, linkedChilds=_queue)
            _queue=set()
            _parentPrev=ids
            continue
         _parent=ids[:-1]
         if _parent!=_parentPrev:
            if _queue and _parentPrev and self.__isEnabled(_parentPrev):
               self._markInIndex(_parentPrev, strictMode=True, linkedChilds=_queue)
            _queue=self._findInIndex(_parent, strictMode=True, calcProperties=False, skipLinkChecking=True)[1].get('linkedChilds', set())
            _parentPrev=_parent
         _link=props.get('link')
         if _link:
            _queue.add(_link)
      #
      if _queue and _parentPrev and self.__isEnabled(_parentPrev):
         self._markInIndex(_parentPrev, strictMode=True, linkedChilds=_queue)

   def _linkModified(self, ids, props, branch, wasExisted, oldLink, doLinkedChilds=None, **kwargs):
      _status=super(DBMatchableLinks, self)._linkModified(ids, props, branch, wasExisted, oldLink, **kwargs)
      if _status is not None and len(ids)>1:
         stopwatch=self.stopwatch('_linkModified.linkedChilds@DBMatchableLinks')
         _parent=ids[:-1]
         if (doLinkedChilds is None and self.__isEnabled(_parent)) or doLinkedChilds:
            #? для `_findInIndex()` есть механизм parentsChain, который позволяет получить всю цепочку, а не только искомый обьект. полезно иметь тоже самое для `_markInIndex()` и особенно для хука `_linkModified()`
            _props=self._findInIndex(_parent, strictMode=True, calcProperties=False, skipLinkChecking=True)[1]
            if 'linkedChilds' not in _props:
               if _status=='CREATED' or _status=='EDITED':
                  self._markInIndex(_parent, linkedChilds=set((props['link'],)))
            else:
               linkedChilds=None
               if (_status=='REMOVED' or _status=='EDITED') and oldLink in _props['linkedChilds']:
                  if len(_props['linkedChilds'])==1: linkedChilds=set()
                  else:
                     linkedChilds=_props['linkedChilds'].copy()
                     linkedChilds.remove(oldLink)
               if (_status=='CREATED' or _status=='EDITED') and props['link'] not in _props['linkedChilds']:
                  if linkedChilds is None:
                     linkedChilds=_props['linkedChilds'].copy()
                  linkedChilds.add(props['link'])
               if linkedChilds is not None:
                  self._markInIndex(_parent, linkedChilds=linkedChilds)
         stopwatch()
      return _status

   def getLinked(self, ids, props=None, strictMode=True, safeMode=True):
      if props is None:
         badLinkChain=[]
         try:
            isExist, props, _=self._findInIndex(ids, strictMode=True, calcProperties=True, linkChain=badLinkChain)
         except BadLinkError:
            # удаляем плохой линк
            for _ids, _props in reversed(badLinkChain):
               self.set(_ids, None, existChecked=_props, allowForceRemoveChilds=True)
            raise StopIteration
         if not isExist:
            if strictMode:
               raise NotExistError(ids)
            else:
               return set()
      if not props or 'linkedChilds' not in props or not props['linkedChilds']:
         return set()
      return props['linkedChilds'].copy() if safeMode else props['linkedChilds']

   #? поскольку prop@backlink хранится также в виде сэта, данная функция может работать и с ним што может быть весьма полезным. однако нужна возможность обратиться к данной пропе по идсам (сейчас при передаче идсов автоматически берутся linkedChilds). возможно не стоит так сильно позиционировать данное расширение именно для работы с линкедчайлд, и сделать его более универсальным - хотябы для работы с бэклинками.
   def matchLinks(self, idsList, like='and', skipEmpty=False, skipNotExists=True, strictMode=True, skipLinkChecking=False):
      if not callable(like):
         if like not in self.__matchMethodConvMap:
            raise ValueError('Unknown "like" method, supported only: '+(', '.join('"%s"'%k for k in self.__matchMethodConvMap)))
         stopwatch=self.stopwatch('matchLinks.prepare(%i)@DBMatchableLinks'%(len(idsList)))
         m=self.__matchMethodConvMap[like]
      else:
         m=like
      _first, _other=None, []
      _isMethodName=not callable(m)
      for ids in idsList:
         if isinstance(ids, set):
            linkedChilds=ids
         else:
            isExist, props, _=self._findInIndex(ids, strictMode=strictMode, calcProperties=False, skipLinkChecking=skipLinkChecking)
            if not isExist:
               if skipNotExists: continue
               else: props={}
            linkedChilds=props.get('linkedChilds')
            if not linkedChilds:
               if skipEmpty: continue
               else: linkedChilds=set()  #? теоретически здесь можно добавить оптимизацию взависимости от `like`, но врятли игра стоит свечь
         if _isMethodName and _first is None:
            _first=linkedChilds
         else:
            _other.append(linkedChilds)
      stopwatch()
      #
      if not _other:
         if not _isMethodName and m=='union' and _first is not None:
            return _first
         return set()
      stopwatch=self.stopwatch('matchLinks.%s(%i)@DBMatchableLinks'%(like, len(idsList)))
      if _isMethodName:
         m=getattr(_first, m)
      res=m(*_other)
      stopwatch()
      return res
