# -*- coding: utf-8 -*-
import sys
sys.path.append('/var/python/libs/')
sys.path.append('/var/python/')
sys.path.append('/home/python/libs/')
sys.path.append('/home/python/')
from flaskJSONRPCServer import flaskJSONRPCServer
from functionsex import *
from logger import LoggerLocal
from VombatiDB import VombatiDB, showDB, showStats

class ScreendeskTestDB:
   def __init__(self, workspace):
      self.workspace=workspace
      path=getScriptPath(real=True, f=__file__)+'/tmp/db1'
      # print path, raw_input()
      self.db=VombatiDB(('NS', 'Columns', 'StorePersistentWithCache', 'Search'))(self.workspace, path)
      self.db.settings.flushOnChange=False
      self.db.settings.columns_default_allowUnknown=False
      self.db.settings.columns_default_allowMissed=False
      self.db.settings.dataMerge_ex=True
      self.db.settings.dataMerge_deep=False
      # self.configNS()
      self.db.connect()

   def configNS(self):
      self.db.configureNS([
      #  (Name, (Parents, Childs, Columns, AllowOnlyIndexed)) # noqa
         ('tmp', (None, None, None, False)),
         ('user', (None, ['operator', 'project'], {'email':'str', 'name':'str'})),
         ('operator', (['user', 'project'], ['project'], {'__needed':False, '__allowUnknown':True, 'login':'str'})),
         ('project', (['user', 'operator'], ['operator'], {'url':'str', 'type':'str'})),
      ], andClear=True)

   def modifyProp(self, ids, data):
      for ids, (props, l) in self.db.iterBranch(ids, calcProperties=False):
         props.update(data)

   def show(self):
      showDB(self.db)

   def _clearDB(self):
      self.db.truncate()
      self.configNS()
      # for ids, (props, l) in self.db.iterBranch(recursive=False, safeMode=True, calcProperties=False):
      #    print '-', ids
      #    self.db.remove(ids, existChecked=props)

   def _makeLongLink(self, root=None, l=10):
      if root is None:
         self.db.set('root', {'d':'root'})
         chain=('root',)
      else:
         chain=root if isTuple(root) else (root,)
      for i in xrange(l):
         k='_tmpLink%s'%i
         self.db.link(chain+(k,), chain)
         chain=chain+(k,)
      return chain

   def _fillDB(self, usersMax=10, projectsMax=5, operatorMax=10):
      for iUser in xrange(usersMax):
         iUser+=1
         # self.db.set('user%s'%iUser, {}, allowMerge=False)
         self.db.set('user%s'%iUser, {'name':'User %s'%iUser, 'email':'user_%s@test.ru'%iUser}, allowMerge=True)
         #
         projectCount=1+int(random.random()*projectsMax)
         projects=[]
         for iProj in xrange(projectCount):
            iProj+=1
            idProj=('user%s'%iUser, 'project%s'%iProj)
            # self.db.set(idProj, {}, allowMerge=False)
            self.db.set(idProj, {'type':'site', 'url':'example.com'}, allowMerge=True)
            projects.append(idProj)
         #
         operatorCount=1+int(random.random()*operatorMax)
         operators=[]
         for iOpr in xrange(operatorCount):
            iOpr+=1
            idOpr=('user%s'%iUser, 'operator%s'%iOpr)
            self.db.set(idOpr, {}, allowMerge=False)
            self.db.set(idOpr, {'login':'op%s'%iOpr}, allowMerge=True)
            operators.append(idOpr)
         #
         if not projects or not operators: continue
         links=[]
         while len(links)<max(len(projects), len(operators)):
            ids1=random.choice(operators)
            ids2=random.choice(projects)
            if (ids1, ids2) in links or (ids2, ids1) in links: continue
            links.append((ids1, ids2))
            self.db.link(list(ids1)+[ids2[-1]], ids2)
            self.db.link(list(ids2)+[ids1[-1]], ids1)

   def speedtestWrite(self):
      print '-'*25, 'Write speed-tests', '-'*25
      print 'add random roots'
      tArr=[]
      for i in xrange(10000):
         tArr.append(randomEx(vals=tArr, pref='__tmp'))
      f=lambda: self.db.set(ids, {})
      try:
         tArr1=[timeit.timeit(f, number=1) for ids in tArr]
         s='%s objects: min=%s, mid=%s, max=%s'
         print s%(len(tArr), time2human(min(tArr1), inMS=False), time2human(arrMedian(tArr1), inMS=False), time2human(max(tArr1), inMS=False))
      finally:
         for s in tArr:
            self.db.remove(s)
      lll=16
      print 'long-link(%s) modify with NS check'%lll
      root='_ll'+randomEx()+randomEx()+randomEx()
      self.db.set(root, {'k1':'test', 'k2':314})
      try:
         ids=self._makeLongLink(root=root, l=lll)
         self.db.setNS('_tmpLink', columns={'k1':'str', 'k2':'int'})
         timeitMe(lambda: self.db.set(root, {'k1':str(time.time()), 'k2':int(time.time())}), n=100, m=1)
      finally:
         self.db.remove(root)
         self.db.delNS('_tmpLink', strictMode=False, allowCheckIndex=False)
      print

   def speedtestRead(self):
      print '-'*25, 'Read speed-tests', '-'*25
      lll=16
      print 'long-link(%s) access'%lll
      root='_ll'+randomEx()+randomEx()+randomEx()
      self.db.set(root, {'data':'root'})
      try:
         ids=self._makeLongLink(root=root, l=lll)
         assert self.db.get(root, returnRaw=True, strictMode=True)==self.db.get(ids, returnRaw=True, strictMode=True), 'Long link broken!'
         timeitMe(lambda: self.db.get(ids, returnRaw=True, strictMode=True), n=100, m=1)
      finally:
         self.db.remove(root)
      tEnv={'tFunc1':lambda: True}
      q={'what':'NS, IDS, DATA', 'branch':None, 'where':'NS=="project" and tFunc1()', 'env':tEnv, 'allowCache':True, 'checkIsEmpty':False}
      res=tuple(self.db.query(what='NS, IDS, DATA', branch=None, where='NS=="project" and tFunc1()', env=tEnv, allowCache=False))
      qF=bind(self.db.query, q)
      print '\nwarming query cache'
      for i in xrange(5):
         mytime=time.time()
         r=qF()
         print '>>%i, %s'%(i, time2human(time.time()-mytime, inMS=False))
         assert tuple(r)==res
         time.sleep(0.05)
      print '\ncompiling query empty Env vs globals'
      q=self.db.queryPrep('NS, IDS, DATA', None, 'NS=="project"', allowCache=False)
      timeitMe(bind(self.db.queryCompile, {'q':q, 'env':{}}), n=100, m=10)
      timeitMe(bind(self.db.queryCompile, {'q':q, 'env':None}), n=100, m=10)
      print '\niterBranch() treeMode vs linear'
      for f in [
         bind(self.db.iterBranch, {'ids':None, 'recursive':True, 'treeMode':True, 'safeMode':True, 'offsetLast':False, 'calcProperties':True}),
         bind(self.db.iterBranch, {'ids':None, 'recursive':True, 'treeMode':False, 'safeMode':True, 'offsetLast':False, 'calcProperties':True}),
      ]: timeitMe(lambda f=f: tuple(f()), n=100, m=10)
      print '\niterBranch() vs query()'
      for f in [
         bind(self.db.iterBranch, {'ids':None, 'recursive':True, 'treeMode':True, 'safeMode':True, 'offsetLast':False, 'calcProperties':True}),
         bind(self.db.query, {'what':None, 'branch':None, 'where':None, 'env':None, 'allowCache':True, 'checkIsEmpty':False, 'calcProperties':True, 'recursive':True, 'returnRaw':True}),
      ]: timeitMe(lambda f=f: tuple(f()), n=100, m=10)
      print '\ncompiling query without and with `precompile`'
      timeitMe(bind(self.db.queryCompile, {'q':self.db.queryPrep(None, None, 'NS=="project" and tFunc1()', precompile=False, allowCache=False), 'env':tEnv}), n=100, m=10)
      timeitMe(bind(self.db.queryCompile, {'q':self.db.queryPrep(None, None, 'NS=="project" and tFunc1()', precompile=True, allowCache=False), 'env':tEnv}), n=100, m=10)
      print '\nquery prepared vs compiled'
      timeitMe(bind(self.db.query, {'q':self.db.queryPrep(None, None, 'NS=="project" and tFunc1()'), 'env':tEnv}), n=100, m=10)
      timeitMe(bind(self.db.query, {'q':self.db.queryCompile(self.db.queryPrep(None, None, 'NS=="project" and tFunc1()'), env=tEnv), 'env':tEnv}), n=100, m=10)
      print

   def test(self):
      # self.modifyProp(None, {'branchModified':False})
      self.show()
      if self.workspace.server._raw_input('Clear DB? ')=='y': self._clearDB()
      if self.workspace.server._raw_input('Fill DB with new random data? ')=='y':
         _cUsers=intEx(self.workspace.server._raw_input('Max users: '))
         _cProjs=intEx(self.workspace.server._raw_input('Max projects: '))
         _cOps=intEx(self.workspace.server._raw_input('Max operators: '))
         self._fillDB(_cUsers, _cProjs, _cOps)
      if self.workspace.server._raw_input('Create long chain of links? ')=='y':
         l=intEx(self.workspace.server._raw_input("Chain's length: "))
         self._makeLongLink(l)
      if self.workspace.server._raw_input('Run speed-tests? ')=='y':
         self.speedtestWrite()
         self.speedtestRead()

      # timeitMe(lambda:self.db.get(('root', 'k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9')), n=1000, m=1)
      # self.db._speedStats.clear()
      # f=bind(self.db.iterBranch, {'ids':None, 'recursive':True, 'treeMode':True, 'safeMode':True, 'offsetLast':False, 'calcProperties':True})
      # timeitMe(lambda f=f: tuple(f()), n=1000, m=1)
      # showStats(self.db)
      # raw_input()

      print '-'*25, 'Searching test', '-'*25
      tEnv={'tFunc1':lambda: True}
      q=self.db.queryPrep('NS, IDS, DATA.items()', None, 'NS=="project" and not DB.isLink(PROPS) and tFunc1()')
      q=self.db.queryCompile(q, env=tEnv)
      print 'Dump of compiled query:', q.dump()
      res=self.db.query(q=q, env=tEnv)
      print 'Search results:'
      if not res:
         print 'No data finded'
      else:
         tArr=tuple(res)
         print 'Finded %i objects:'%len(tArr)
         if len(tArr)<15:
            for o in tArr: print o
         else:
            for o in tArr[:5]: print o
            print '-- more %i items --'%(len(tArr)-10)
            for o in tArr[-5:]: print o

      #! нужен тест - итерация ветки, которую сразу после создания итератора удалят. такая возможность нужна для очистки веток в фоновом режиме при удалении
      # self.db.set(('user1', '?operator'), {}, allowMerge=False)
      # self.db.set('tmp', {'test':1}, allowMerge=False)
      # for i, o in enumerate([
      #    ('user1', 'operator4'),
      #    ('user1', 'project2', 'operator4'),
      #    ('user1', 'project1', 'operator4'),
      #    ('tmp', 't1'),
      # ]):
      #    ids=('tmp', 't%s'%i)
      #    self.db.link(ids, o)

      # self.db.link(('tmp', 't1'), ('user1', 'operator4'))
      # self.db.remove(('user1', 'operator4'))

      print '\nTEST_ENDED'
      if self.workspace.server._raw_input('Show data again? ')=='y': self.show()
      if console.inTerm() and self.workspace.server._raw_input('Run interactive mode? ')=='y': console.interact(scope=locals())
      showStats(self.db)

if __name__ == '__main__':
   ErrorHandler()
   workspace=MagicDict()
   workspace.server=flaskJSONRPCServer(None, gevent=True, log=4, tweakDescriptors=[1000, 1000], experimental=True, controlGC=False)
   workspace.logger=LoggerLocal(app='VombatiDB-TestSuite', server=workspace.server, maxLevel=4)
   workspace.logger.settings.allowExternalNull=False
   workspace.logger.settings.lengthSolong=2048
   workspace.log=workspace.logger.log
   # start testing
   ScreendeskTestDB(workspace).test()
