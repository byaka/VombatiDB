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
import gc

def __init():
   return DBStorePersistentWithCache, ('DBStorePersistentWithCache', 'StorePersistentWithCache')

class DBStorePersistentWithCache(DBBase):
   def _init(self, path, *args, **kwargs):
      self.supports.data=True
      self.supports.inMemoryData=True
      self.supports.detailedDiffData=True
      self.supports.persistentData=True
      self.supports.persistentMeta=True
      self.supports.persistentProps=True
      self.supports.picklingData=True
      self.supports.picklingMeta=True
      self.supports.picklingProps=True
      #
      # self.settings.store_flushOnChange=False  #! implement
      self.settings.store_flushOnExit=True
      # self.settings.store_flushAuto=False  #! implement
      self.settings.path=path
      self.settings.store_controlGC=True
      self.__store=MagicDict({
         'loaded':False,
         'writeCount':0,
      })
      self.__skip_saveChanges=False
      self.__data_lock=self.workspace.rlock()
      self.__meta_lock=self.workspace.rlock()
      self.__flushQueue={}
      self.__c_OBJECT_REPLACED=object()  # special const for indicate, that object fully replaced
      return super(DBStorePersistentWithCache, self)._init(*args, **kwargs)

   def _reset(self, *args, **kwargs):
      super(DBStorePersistentWithCache, self)._reset(*args, **kwargs)
      if not os.path.isdir(self._settings['path']):
         raise ValueError('Path for fs-store not exist: "%s"'%self._settings['path'])
      self.__cache={}
      self._changed={}
      self._loadedStore(self.__cache)

   def _loadedStore(self, data):
      pass

   def _backupToStore(self, files, name=''):
      if not files: return None
      mytime=getms(inMS=True)
      name=name or '%s.zip'%datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
      self.workspace.log(4, 'Backuping fs-store (%s files): %s'%(len(files), name))
      path=os.path.join(self._settings['path'], 'backup')
      fbk=os.path.join(path, name)
      try: os.mkdir(path)
      except OSError: pass
      zipWrite(fbk, files, mode='w', forceCompression=False, silent=False)
      self.workspace.log(3, 'Backuped fs-store (%s files) in %ims: %s'%(len(files), getms(inMS=True)-mytime, name))
      return True

   def _connect(self, strictMode=True, needBackup=True, needRebuild=True, andMeta=True, **kwargs):
      self.workspace.log(4, 'Loading fs-store from "%s"'%(self._settings['path']))
      _gcWasEnabled=self._settings['store_controlGC'] and gc.isenabled()
      if _gcWasEnabled: gc.disable()
      mytime=getms(inMS=True)
      filesForRemove={}
      filesForBackup={}
      if andMeta:
         self._loadMetaFromStore(filesForRemove, filesForBackup, strictMode=strictMode)
      self._loadDataFromStore(filesForRemove, filesForBackup, strictMode=strictMode)
      # теперь мы бекапим и удаляем старые данные и делаем свежий дамп
      if needBackup:
         self._backupToStore(filesForBackup)
      if needRebuild and filesForRemove:
         mytime2=getms(inMS=True)
         self.workspace.log(4, 'Rebuilding fs-store, removing old (%s files)'%len(filesForRemove))
         folderClear(self._settings['path'], alsoFiles=True, alsoDirs=False, silent=False, filter=filesForRemove, isBlacklist=False)
         self.snapshot(needBackup=False)
         self.workspace.log(3, 'Rebuilded fs-store in %ims'%(getms(inMS=True)-mytime2))
      self.workspace.log(3, 'Loading fs-store from "%s" in %ims'%(self._settings['path'], getms(inMS=True)-mytime))
      super(DBStorePersistentWithCache, self)._connect(strictMode=strictMode, **kwargs)
      if _gcWasEnabled: gc.enable()
      self.__store.loaded=True

   def _checkFileFromStore(self, f):
      if f=='meta': f='meta.dat'
      elif f=='data': f='data.dat'
      fp=os.path.join(self._settings['path'], f)
      s=os.path.isfile(fp)
      return f, fp, s

   def _loadMetaFromStore(self, toRemove, toBackup, strictMode=True):
      self.workspace.log(4, 'Loading Meta from fs-store')
      mytime=getms(inMS=True)
      fn, fp, fExist=self._checkFileFromStore('meta')
      if not fExist: return None
      with open(fp, 'r') as f:
         f=self.workspace.fileWrap(f)
         data=f.read()
      self._parseMeta(data)
      self.workspace.log(3, 'Loaded Meta from fs-store in %ims'%(getms(inMS=True)-mytime))
      toBackup[fn]=data
      return 1

   def _loadDataFromStore(self, toRemove, toBackup, strictMode=True):
      self.workspace.log(4, 'Loading Data from fs-store')
      mytime=getms(inMS=True)
      fn, fp, fExist=self._checkFileFromStore('data')
      if not fExist: return None
      self.__skip_saveChanges=True
      notCreatedQueue={}
      backlinkQueue=defaultdict(set)
      with open(fp, 'rU') as f:
         curKeyLen, maxKeyLen=1, 1
         while curKeyLen<=maxKeyLen:
            f.seek(0)
            ids, props, skip=None, None, 0
            for line in f:
               if skip:
                  skip-=1
                  continue
               line=line.decode('utf-8')
               if line and line[-1]=='\n': line=line[:-1]
               if not line and ids is None: pass  # just empty line
               elif ids is None:
                  _ids=tuple(line.strip('\t').split('\t'))
                  maxKeyLen=max(maxKeyLen, len(_ids))
                  if len(_ids)!=curKeyLen:
                     # too short for current iteration, we process it later
                     skip=2
                     continue
                  ids=_ids
               elif props is None:
                  props={} if not line else pickle.loads(line.decode('string_escape'))
                  props.pop('backlink', None)  # fixing old db-versions
                  if ids in backlinkQueue:
                     props['backlink']=backlinkQueue.pop(ids)
               else:
                  try:
                     if line=='-':
                        self._unmarkInIndex(ids, **props)
                        notCreatedQueue[ids]=0
                        isExist=False
                     else:
                        isExist=self._markInIndex(ids, strictMode=True, skipBacklinking=True, **props)
                  except ParentNotExistError:
                     if line!='-':
                        notCreatedQueue[ids]=1
                  else:
                     if line=='-':
                        allowMerge, data=False, None
                     elif line=='@':
                        allowMerge, data=False, True
                     else:
                        allowMerge, data=(True, line[1:]) if line and line[0]=='+' else (False, line)
                        data=pickle.loads(data.decode('string_escape')) if data else {}
                     tArr=((ids, (isExist, data, allowMerge, props, {})),)
                     self._setData(tArr)
                     if 'link' in props:
                        ids2=props['link']
                        isExist2, props2, _=self._findInIndex(ids2, strictMode=False, calcProperties=False, skipLinkChecking=True)
                        if isExist2:
                           if 'backlink' in props2:
                              props2['backlink'].add(ids)
                           else:
                              props2['backlink']=set((ids,))
                        else:
                           backlinkQueue[ids2].add(ids)
                  #
                  ids, props=None, None
            curKeyLen+=1
      #
      for _ids, isNotOk in notCreatedQueue.iteritems():
         if not isNotOk: continue
         if strictMode:
            raise ParentNotExistError('(unknown) for %s'%(_ids,))
         self.workspace.log(1, 'Branch %s skipped due some parents not exist'%(_ids,))
      self.workspace.log(3, 'Loaded Data from fs-store in %ims'%(getms(inMS=True)-mytime))
      toBackup[fn]=(open, fp)
      toRemove[fn]=True
      self.__skip_saveChanges=False
      return 1

   def _markInIndex(self, ids, _changes=None, **kwargs):
      if _changes is not None:
         raise NotImplementedError
      _changes={} if not self.__skip_saveChanges else None
      r=super(DBStorePersistentWithCache, self)._markInIndex(ids, _changes=_changes, **kwargs)
      if _changes:
         stopwatch=self.stopwatch('_markInIndex@DBStorePersistentWithCache')
         _flushQueue=self.__flushQueue
         # сохраняем в очередь все Props, а на этапе сохранения отфильтруем персистентные
         for ids, tDiff in _changes.iteritems():
            if ids not in _flushQueue:
               t=timetime()
               _flushQueue[ids]=[tDiff, False, t, t]
            else:
               o=_flushQueue[ids]
               if o[0]:
                  o[0].update(tDiff)
               else:
                  o[0]=tDiff
               o[-1]=timetime()
         stopwatch()
      return r

   def _unmarkInIndex(self, ids, _changes=None, **kwargs):
      if _changes is not None:
         raise NotImplementedError
      _changes={} if not self.__skip_saveChanges else None
      r=super(DBStorePersistentWithCache, self)._unmarkInIndex(ids, _changes=_changes, **kwargs)
      if _changes:
         stopwatch=self.stopwatch('_unmarkInIndex@DBStorePersistentWithCache')
         _flushQueue=self.__flushQueue
         # сохраняем в очередь все Props, а на этапе сохранения отфильтруем персистентные
         for ids, tDiff in _changes.iteritems():
            if ids not in _flushQueue:
               t=timetime()
               _flushQueue[ids]=[tDiff, False, t, t]
            else:
               o=_flushQueue[ids]
               if o[0]:
                  o[0].update(tDiff)
               else:
                  o[0]=tDiff
               o[-1]=timetime()
         stopwatch()
      return r

   def _setData(self, items, **kwargs):
      stopwatch=self.stopwatch('_setData@DBStorePersistentWithCache')
      changes={}
      _skip_saveChanges=self.__skip_saveChanges
      _flushQueue=self.__flushQueue
      for ids, (isExist, data, allowMerge, props, propsUpdate) in items:
         if data is not None and data is not True and not isinstance(data, dict):
            stopwatch()
            raise ValueError('Incorrect format for %s'%(ids,))
         tDiff=changes[ids]=self._saveToCache(ids, isExist, data, allowMerge, props, propsUpdate)
         if _skip_saveChanges: continue
         elif tDiff is False: pass
         elif ids not in _flushQueue:
            t=timetime()
            _flushQueue[ids]=[{}, tDiff, t, t]
         else:
            o=_flushQueue[ids]
            if not isinstance(tDiff, dict) or not o[1] or (o[1] is not self.__c_OBJECT_REPLACED and not isinstance(o[1], dict)): o[1]=tDiff
            elif o[1] is self.__c_OBJECT_REPLACED:
               o[1]=self.__cache[ids]
            else:
               o[1].update(tDiff)
            o[-1]=timetime()
      stopwatch()
      return changes

   def _saveToCache(self, ids, isExist, data, allowMerge, props, propsUpdate):
      isExist=isExist and ids in self.__cache
      old=isExist and self.__cache[ids]
      if isExist and old==data:
         return False
      elif data is None:
         if isExist:
            del self.__cache[ids]
            return None
      elif data is not True and isExist and allowMerge and old is not True:
         diff={}
         isEx=self._dataMerge(old, data, changed=diff, changedType='new')
         if isEx:
            if not diff:
               return False
            else:
               return diff
         else:
            return self.__c_OBJECT_REPLACED
      else:
         self.__cache[ids]=data
         return self.__c_OBJECT_REPLACED

   def _getData(self, ids, props, **kwargs):
      res=self.__cache.get(ids, False)
      if isinstance(res, dict): res=res.copy()
      return res

   def _close(self, *args, **kwargs):
      super(DBStorePersistentWithCache, self)._close(*args, **kwargs)
      if self._settings['store_flushOnExit'] and self.__store.loaded:
         self.flush(andMeta=True, andData=True, **kwargs)

   #! думаю этот метод нужно переименовать, поскольку механизм снапшотов скарее всего будет добавлен в ядро
   def snapshot(self, needBackup=True, strictMode=False, **kwargs):
      self.workspace.log(4, 'Making snapshot of DB to fs-store')
      _gcWasEnabled=self._settings['store_controlGC'] and gc.isenabled()
      if _gcWasEnabled: gc.disable()
      mytime=getms(inMS=True)
      try:
         if needBackup:
            filesForBackup=[]
            for fn in ('meta', 'data'):
               fn, fp, fExist=self._checkFileFromStore(fn)
               if not fExist: continue
               filesForBackup[fn]=(open, fp)
            self._backupToStore(filesForBackup)
         self._saveMetaToStore(**kwargs)
         #
         mytime2=getms(inMS=True)
         self.workspace.log(4, 'Saving Data to fs-store')
         propRules=self._getPropMap()[1]
         c=0
         fp=self._checkFileFromStore('data')[1]
         with self.__data_lock, open(fp, 'w') as f:
            self.workspace.fileWrap(f)
            for ids, (props, l) in self.iterBranch(treeMode=False, calcProperties=False):
               try:
                  _ids='\t'.join(ids)
                  data=self.__cache[ids]
                  if data is None: _data='-'
                  elif data is True: _data='@'
                  else:
                     _data=pickle.dumps(data, pickle.HIGHEST_PROTOCOL).encode('string_escape')
                  _props={k:v for k,v in props.iteritems() if k in propRules['persistent']}
                  _props=pickle.dumps(_props, pickle.HIGHEST_PROTOCOL).encode('string_escape') if _props else ''
                  line='\n%s\n%s\n%s'%(_ids, _props, _data)
                  line=line.encode('utf-8')
                  f.write(line)
                  c+=1
               except Exception:
                  if strictMode: raise
                  self.workspace.log(1, 'Error while saving %s: %s'%(ids, getErrorInfo()))
         self.workspace.log(3, 'Saved Data to fs-store (%i items) in %ims'%(c, getms(inMS=True)-mytime2))
         self.workspace.log(3, 'Maked snapshot of DB to fs-store in %ims'%(getms(inMS=True)-mytime))
      finally:
         if _gcWasEnabled: gc.enable()

   def flush(self, andMeta=True, andData=True, **kwargs):
      self.workspace.log(4, 'Saving changes to fs-store')
      _gcWasEnabled=self._settings['store_controlGC'] and gc.isenabled()
      if _gcWasEnabled: gc.disable()
      mytime=getms(inMS=True)
      try:
         if andMeta:
            self._saveMetaToStore(**kwargs)
         if andData and self.__flushQueue:
            tArr=None
            mytime2=getms(inMS=True)
            self.workspace.log(4, 'Saving Data to fs-store')
            propRules=self._getPropMap()[1]
            c=0
            fp=self._checkFileFromStore('data')[1]
            with self.__data_lock, open(fp, 'a+') as f:
               self.workspace.fileWrap(f)
               tArr, self.__flushQueue=self.__flushQueue.copy(), {}
               for ids, (propDiff, dataDiff, timeC, timeM) in tArr.iteritems():
                  if dataDiff is None: _data='-'
                  elif isinstance(dataDiff, dict):
                     _data='+'+pickle.dumps(dataDiff, pickle.HIGHEST_PROTOCOL).encode('string_escape')
                  elif not dataDiff: _data='+'
                  elif dataDiff is self.__c_OBJECT_REPLACED:
                     data=self.__cache[ids]
                     if data is None: _data='-'
                     elif data is True: _data='@'
                     else:
                        _data=pickle.dumps(data, pickle.HIGHEST_PROTOCOL).encode('string_escape')
                  #
                  if not propDiff: _props=''
                  else:
                     _props={k:propDiff[k] for k in propDiff if k in propRules['persistent']}
                     _props=pickle.dumps(_props, pickle.HIGHEST_PROTOCOL).encode('string_escape') if _props else ''
                  #
                  if not _props and _data=='+': continue
                  _ids='\t'.join(ids)
                  line='\n%s\n%s\n%s'%(_ids, _props, _data)
                  line=line.encode('utf-8')
                  f.write(line)
                  c+=1
            self.workspace.log(3, 'Saved Data to fs-store (%i items) in %ims'%(c, getms(inMS=True)-mytime2))
            self.__store['writeCount']+=1
            #! on error we need to dump changes (tArr and self.__flushQueue) somewhere
         self.workspace.log(3, 'Saved changes to fs-store in %ims'%(getms(inMS=True)-mytime))
      finally:
         if _gcWasEnabled: gc.enable()

   def _saveMetaToStore(self, **kwargs):
      self.workspace.log(4, 'Saving Meta to fs-store')
      mytime=getms(inMS=True)
      data=self._dumpMeta()
      self.workspace.log(3, 'Dumped meta in %ims'%(getms(inMS=True)-mytime))
      fp=self._checkFileFromStore('meta')[1]
      with self.__meta_lock, open(fp, 'w') as f:
         self.workspace.fileWrap(f)
         f.write(data)
      self.workspace.log(3, 'Saved Meta to fs-store in %ims'%(getms(inMS=True)-mytime))

   def truncate(self, **kwargs):
      tArr=set()
      for s in ('meta', 'data'):
         fn, fp, fExist=self._checkFileFromStore(s)
         if fExist: tArr.add(fn)
      if tArr:
         folderClear(self._settings['path'], alsoFiles=True, alsoDirs=False, silent=False, filter=tArr, isBlacklist=False)
      return super(DBStorePersistentWithCache, self).truncate(**kwargs)
