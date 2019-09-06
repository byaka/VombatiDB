# -*- coding: utf-8 -*-
from .utils import *
from __init__ import VombatiDB

def DBTestPriority(f, priority=None):
   def wrapped(*args, **kwargs):
      return f(*args, **kwargs)
   if priority is None:
      #! этот код позволяет получить номер строки декларации функции, но нужно доработать его поддержкой методов класса чтобы корректно отрабатывать, если в одном файле встречаются методы с одинаковыми названиями
      # _ast=ast.parse(fileGet(f.__file__))
      # tArr1={}
      # for oo in ast.walk(_ast):
      #    if not isinstance(oo, ast.ClassDef): continue
      #    if oo.name not in api.tree.classes: continue
      #    tArr1[oo.name]=oo.lineno
      #    tArr2={}
      #    for oo2 in oo.body:
      #       if not isinstance(oo2, ast.FunctionDef): continue
      #       if oo2.name not in api.tree.classes[oo.name].tree.methods.public: continue
      #       tArr2[oo2.name]=oo2.lineno
      priority=-INFINITY
   setattr(wrapped, '__priority', priority)
   return wrapped

DBTESTDEF_extensions=('NS', 'LazyIndex', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search')
DBTESTDEF_settings={
   'store_flushOnChange':False,
   'store_flushAuto':False,
   'store_controlGC':True,
   'columns_default_allowUnknown':False,
   'columns_default_allowMissed':False,
   'dataMerge_ex':True,
   'dataMerge_deep':False,
   'ns_checkIndexOnConnect':True,
   'linkedChilds_default_do':False,
   'linkedChilds_inheritNSFlags':True,
}

#! в некоторых тестах важно менять настройки базы. поскольку без перезапуска базы это сделать нельзя, нужен просто механизм перезапуска, вызываемый прямо из теста
class DBTestBase(object):
   def __init__(self, workspace=None, removeDBAfterTests=True):
      if workspace:
         self.workspace=workspace
      else:
         self.workspace=self._initWorkspace()
      if 'dbPath' in self.workspace:
         self._externalDB=removeDBAfterTests
      self._initDB()

   def _initWorkspace(self):
      workspace=Workspace()
      workspace.dbPath=tempfile.mkdtemp()
      return workspace

   def _initDB(self):
      path=getattr(self.workspace, 'dbPath', None)
      exts=getattr(self.workspace, 'dbExtensions', DBTESTDEF_extensions)
      setts=getattr(self.workspace, 'dbSettings', {})
      useDefSetts=getattr(self.workspace, 'dbSettingsUseDefault', True)
      nsConfig=getattr(self.workspace, 'dbNamespaces', None)
      nsConfigClear=getattr(self.workspace, 'dbNamespacesClearOnInit', True)

      self.db=VombatiDB(exts)(self.workspace, path)

      if useDefSetts:
         setts=dict(DBTESTDEF_settings, **setts)
      for k,v in setts.iteritems():
         self.db.settings[k]=v

      self.configNS(nsConfig, andClear=nsConfigClear)
      self.db.connect()

   def configNS(self, config, andClear=True):
      #  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa
      self.db.configureNS(config, andClear=andClear)

   def close(self):
      if getattr(self, '_externalDB', False): return
      try:
         shutil.rmtree(self.workspace.dbPath)
      except OSError as e:
         # Reraise unless ENOENT: No such file or directory
         if e.errno!=errno.ENOENT: raise getErrorRaw()

   def run(self, **kwargs):
      testMap=[]
      for k in dir(self):
         if not k.startswith('test_'): continue
         o=getattr(self, k)
         if not callable(o): continue
         testMap.append(o)
      testMap.sort(key=lambda o: getattr(o, '__priority', -INFINITY), reverse=True)
      for o in testMap: o()

   def dump(self):
      return dumpDB(self.db)

   def diff(self, dumped):
      return diffDB(self.db, dumped)

   def show(self):
      showDB(self.db)

   def stats(self):
      showStats(self.db)

   def __call__(self, **kwargs):
      return self.run(**kwargs)

   def __enter__(self):
      return self

   def __exit__(self, *err):
      self.close()

def runDBTest(cls, workspace=None, returnDump=False, persistencyCheck=True, **kwargs):
   persistent, dbDumped={}, None
   with cls(workspace) as test:
      persistent={k:test.db.supports.get(k, False) for k in ['persistentData', 'persistentMeta', 'persistentProps']}
      passed=False
      try:
         test.run(**kwargs)
         if returnDump or persistencyCheck:
            dbDumped=test.dump()
         passed=True
      finally:
         if not passed and (not IN_TERM or raw_input(COLORS.bold+COLORS.red+'Test failed, print current DB state? '+COLORS.end)=='y'): test.show()
   if persistencyCheck:
      assert any(persistent.itervalues()), 'Persistency not enabled'
      assert dbDumped, 'DB is empty or dumping have problems'
      #
      with cls(workspace) as test:
         passed=False
         try:
            o=next(test.diff(dbDumped), NULL)
            assert o is NULL, 'DB mismatched: %s (%s)'%(o[1], o[0])
            passed=True
         finally:
            if not passed and (not IN_TERM or raw_input(COLORS.bold+COLORS.red+'Persistency check failed, print current DB state? '+COLORS.end)=='y'): test.show()
   if returnDump:
      return dbDumped
