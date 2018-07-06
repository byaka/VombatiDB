# -*- coding: utf-8 -*-
from ..utils import *
from ..DBBase import DBBase
from gevent.lock import RLock
from gevent.fileobject import FileObjectThread as geventFileObjectThread

def __init():
   return DBStorePersistentWithCache, ('DBStorePersistentWithCache', 'StorePersistentWithCache')

class DBStorePersistentWithCache(DBBase):
   def _init(self, path, *args, **kwargs):
      self.supports.inMemoryData=True
      self.supports.detailedDiffData=True
      self.supports.readableData=True
      self.supports.writableData=True
      self.supports.persistentData=True
      self.supports.persistentMeta=True
      self.supports.writableData=True
      self.supports.picklingProperties=True
      #
      # self.settings.flushOnChange=False
      self.settings.flushOnExit=True
      # self.settings.flushAuto=False
      self.settings.path=path
      self.settings.default_emulateAsync=True
      self.settings.storeMerge_soLong=0.1
      self.settings.storeMerge_sleepTime=0.000001
      self.___fsVars=MagicDict({
         'writeCount':0,
      })
      self._store_data_lock=RLock()
      self._store_meta_lock=RLock()
      self._flushQueue={}
      return super(DBStorePersistentWithCache, self)._init(*args, **kwargs)

   def _reset(self, *args, **kwargs):
      super(DBStorePersistentWithCache, self)._reset(*args, **kwargs)
      if not os.path.isdir(self.settings.path):
         raise ValueError('Path for fs-store not exist: "%s"'%self.settings.path)
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
      path=os.path.join(self.settings.path, 'backup')
      fbk=os.path.join(path, name)
      try: os.mkdir(path)
      except OSError: pass
      zipWrite(fbk, files, mode='w', forceCompression=False, silent=False)
      self.workspace.log(3, 'Backuped fs-store (%s files) in %ims: %s'%(len(files), getms(inMS=True)-mytime, name))
      return True

   def _connect(self, strictMode=True, needBackup=True, needRebuild=True, andMeta=True, **kwargs):
      self.workspace.log(4, 'Loading fs-store from "%s"'%(self.settings.path))
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
         folderClear(self.settings.path, alsoFiles=True, alsoDirs=False, silent=False, filter=filesForRemove, isBlacklist=False)
         self.snapshot(needBackup=False)
         self.workspace.log(3, 'Rebuilded fs-store in %ims'%(getms(inMS=True)-mytime2))
      self.workspace.log(3, 'Loading fs-store from "%s" in %ims'%(self.settings.path, getms(inMS=True)-mytime))
      super(DBStorePersistentWithCache, self)._connect(strictMode=strictMode, **kwargs)

   def _checkFileFromStore(self, f):
      if f=='meta': f='meta.dat'
      elif f=='data': f='data.dat'
      fp=os.path.join(self.settings.path, f)
      s=os.path.isfile(fp)
      return f, fp, s

   def _loadMetaFromStore(self, toRemove, toBackup, strictMode=True):
      self.workspace.log(4, 'Loading Meta from fs-store')
      mytime=getms(inMS=True)
      fn, fp, fExist=self._checkFileFromStore('meta')
      if not fExist: return None
      data=self.workspace.server._fileGet(fp, silent=False)
      self._parseMeta(data)
      self.workspace.log(3, 'Loaded Meta from fs-store in %ims'%(getms(inMS=True)-mytime))
      toBackup[fn]=data
      return 1

   def _loadDataFromStore(self, toRemove, toBackup, strictMode=True):
      self.workspace.log(4, 'Loading Data from fs-store')
      mytime=getms(inMS=True)
      fn, fp, fExist=self._checkFileFromStore('data')
      if not fExist: return None
      with open(fp, 'rU') as f:
         curKeyLen, maxKeyLen=1, 1
         while curKeyLen<=maxKeyLen:
            f.seek(0)
            ids, props, skip=None, None, 0
            for line in f:
               if skip:
                  skip-=1
                  continue
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
               else:
                  try:
                     if line=='-':
                        self._unmarkInIndex(ids, _skipFlushing=True, **props)
                     else:
                        isExist=self._markInIndex(ids, strictMode=True, _skipFlushing=True, **props)
                  except StrictModeError:
                     self.workspace.log(2, 'Branch %s skipped due some parents not exist'%(ids,))
                  else:
                     if line=='-':
                        allowMerge, data=False, None
                     elif line=='@':
                        allowMerge, data=False, True
                     else:
                        allowMerge, data=(True, line[1:]) if line and line[0]=='+' else (False, line)
                        data=self.workspace.server._parseJSON(data) if data else {}
                        if allowMerge and data=={}: continue
                     tArr=((ids, (isExist, data, allowMerge, props, {})),)
                     self._set(tArr, _skipFlushing=True)
                  #
                  ids, props=None, None
            curKeyLen+=1
      self.workspace.log(3, 'Loaded Data from fs-store in %ims'%(getms(inMS=True)-mytime))
      toBackup[fn]=(open, fp)
      toRemove[fn]=True
      return 1

   def _dataFileFind(self):
      ptrn=re.compile(r"[0-9]+([.]{0,1}[0-9]*)\.json", re.U).match
      files=os.listdir(self.settings.path)
      files=[float(f[:-5]) for f in files if ptrn(f)]
      files=sorted(files)
      return files

   def _markInIndex(self, ids, _changes=None, _skipFlushing=False, **kwargs):
      if _changes is not None:
         raise NotImplementedError
      _changes={} if not _skipFlushing else None
      r=super(DBStorePersistentWithCache, self)._markInIndex(ids, _changes=_changes, **kwargs)
      if _changes:
         stopwatch=self.stopwatch('_markInIndex@DBStorePersistentWithCache')
         # сохраняем в очередь все Props, а на этапе сохранения отфильтруем персистентные
         for ids, tDiff in _changes.iteritems():
            if ids not in self._flushQueue:
               self._flushQueue[ids]=[tDiff, False, timetime(), timetime()]
            else:
               o=self._flushQueue[ids]
               if o[0]:
                  o[0].update(tDiff)
               else:
                  o[0]=tDiff
               o[-1]=timetime()
         stopwatch()
      return r

   def _unmarkInIndex(self, ids, _changes=None, _skipFlushing=False, **kwargs):
      if _changes is not None:
         raise NotImplementedError
      _changes={} if not _skipFlushing else None
      r=super(DBStorePersistentWithCache, self)._unmarkInIndex(ids, _changes=_changes, **kwargs)
      if _changes:
         stopwatch=self.stopwatch('_unmarkInIndex@DBStorePersistentWithCache')
         # сохраняем в очередь все Props, а на этапе сохранения отфильтруем персистентные
         for ids, tDiff in _changes.iteritems():
            if ids not in self._flushQueue:
               self._flushQueue[ids]=[tDiff, False, timetime(), timetime()]
            else:
               o=self._flushQueue[ids]
               if o[0]:
                  o[0].update(tDiff)
               else:
                  o[0]=tDiff
               o[-1]=timetime()
         stopwatch()
      return r

   def _set(self, items, _skipFlushing=False, **kwargs):
      stopwatch=self.stopwatch('_set@DBStorePersistentWithCache')
      changes={}
      for ids, (isExist, data, allowMerge, props, propsUpdate) in items:
         if data is not None and data is not True and not isinstance(data, dict):
            stopwatch()
            raise ValueError('Incorrect format for %s'%(ids,))
         tDiff=changes[ids]=self._saveToCache(ids, isExist, data, allowMerge, props, propsUpdate)
         if _skipFlushing: pass
         elif tDiff is False or tDiff=={}: pass
         elif ids not in self._flushQueue:
            self._flushQueue[ids]=[{}, tDiff, timetime(), timetime()]
         else:
            o=self._flushQueue[ids]
            if not isinstance(tDiff, dict) or not o[1] or not isinstance(o[1], dict): o[1]=tDiff
            else:
               o[1].update(tDiff)
            o[-1]=timetime()
      stopwatch()
      return changes

   def _saveToCache(self, ids, isExist, data, allowMerge, props, propsUpdate):
      old=isExist and self.__cache[ids]
      if isExist and old==data: return False
      elif data is None:
         if isExist:
            del self.__cache[ids]
            return None
      elif data is not True and isExist and allowMerge and old is not True:
         diff={}
         isEx=self._dataMerge(old, data, changed=diff, changedType='new')
         if isEx:
            if not diff: return False
            else: return diff
         else:
            return True
      else:
         self.__cache[ids]=data
         return True

   def _get(self, ids, props, **kwargs):
      return self.__cache[ids]

   def close(self, **kwargs):
      super(DBStorePersistentWithCache, self).close(**kwargs)
      if self.settings.flushOnExit:
         self.flush(andMeta=True, andData=True, **kwargs)

   def snapshot(self, needBackup=True, **kwargs):
      self.workspace.log(4, 'Making snapshot of DB to fs-store')
      mytime=getms(inMS=True)
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
      propRules=self._propCompiled
      c=0
      fp=self._checkFileFromStore('data')[1]
      with self._store_data_lock, open(fp, 'w') as f:
         geventFileObjectThread(f)
         for ids, (props, l) in self.iterIndex(treeMode=False, calcProperties=False):
            try:
               _ids='\t'.join(ids)
               data=self.__cache[ids]
               if data is None: _data='-'
               elif data is True: _data='@'
               elif not data: _data=''
               else:
                  _data=self.workspace.server._serializeJSON(data)
               _props={k:v for k,v in props.iteritems() if k in propRules['persistent']}
               _props=pickle.dumps(_props).encode('string_escape') if _props else ''
               line='\n%s\n%s\n%s'%(_ids, _props, _data)
               f.write(line)
               c+=1
            except Exception:
               self.workspace.log(1, 'Error while saving %s: %s'%(ids, getErrorInfo()))
      self.workspace.log(3, 'Saved Data to fs-store (%i items) in %ims'%(c, getms(inMS=True)-mytime2))
      self.workspace.log(3, 'Maked snapshot of DB to fs-store in %ims'%(getms(inMS=True)-mytime))

   def flush(self, andMeta=True, andData=True, **kwargs):
      self.workspace.log(4, 'Saving changes to fs-store')
      mytime=getms(inMS=True)
      if andMeta:
         self._saveMetaToStore(**kwargs)
      if andData and self._flushQueue:
         tArr=None
         mytime2=getms(inMS=True)
         self.workspace.log(4, 'Saving Data to fs-store')
         propRules=self._propCompiled
         c=0
         fp=self._checkFileFromStore('data')[1]
         try:
            with self._store_data_lock, open(fp, 'a+') as f:
               geventFileObjectThread(f)
               tArr, self._flushQueue=self._flushQueue.copy(), {}
               for ids, (propDiff, dataDiff, timeC, timeM) in tArr.iteritems():
                  if dataDiff is None: _data='-'
                  elif not dataDiff: _data='+'
                  elif isinstance(dataDiff, dict):
                     _data='+'+self.workspace.server._serializeJSON(dataDiff)
                  elif dataDiff is True:
                     data=self.__cache[ids]
                     if data is None: _data='-'
                     elif data is True: _data='@'
                     elif not data: _data=''
                     else:
                        _data=self.workspace.server._serializeJSON(data)
                  #
                  if not propDiff: _props=''
                  else:
                     _props={k:v for k,v in propDiff.iteritems() if k in propRules['persistent']}
                     _props=pickle.dumps(_props).encode('string_escape') if _props else ''
                  #
                  if not _props and _data=='+': continue
                  _ids='\t'.join(ids)
                  line='\n%s\n%s\n%s'%(_ids, _props, _data)
                  f.write(line)
                  c+=1
            self.workspace.log(3, 'Saved Data to fs-store (%i items) in %ims'%(c, getms(inMS=True)-mytime2))
         except Exception:
            #! here we need to dump changes (tArr and self._flushQueue) somewhere
            raise
      self.workspace.log(3, 'Saved changes to fs-store in %ims'%(getms(inMS=True)-mytime))

   def _saveMetaToStore(self, **kwargs):
      self.workspace.log(4, 'Saving Meta to fs-store')
      mytime=getms(inMS=True)
      fp=self._checkFileFromStore('meta')[1]
      with self._store_meta_lock:
         data=self._dumpMeta()
         self.workspace.log(3, 'Dumped meta in %ims'%(getms(inMS=True)-mytime))
         self.workspace.server._fileWrite(fp, data, mode='w', silent=False, buffer=0)
      self.workspace.log(3, 'Saved Meta to fs-store in %ims'%(getms(inMS=True)-mytime))

   def truncate(self, **kwargs):
      tArr=set()
      for s in ('meta', 'data'):
         fn, fp, fExist=self._checkFileFromStore(s)
         if fExist: tArr.add(fn)
      if tArr:
         folderClear(self.settings.path, alsoFiles=True, alsoDirs=False, silent=False, filter=tArr, isBlacklist=False)
      return super(DBStorePersistentWithCache, self).truncate(**kwargs)
