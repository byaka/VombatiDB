# -*- coding: utf-8 -*-
#~ importing lib, placed near us - not system
import sys, os
p=os.path.normpath(os.path.realpath(__file__)).split(os.path.sep)
sys.path.insert(0, os.path.sep.join(p[:-3]))
m=__import__(p[-3])
for k in ('VombatiDB', 'Workspace', 'WorkspaceOld', 'showDB', 'showStats', 'loadTree', 'dumpTree', 'dumpDB', 'diffDB', 'errors', 'COLORS', 'IN_TERM', 'DBTestBase', 'DBTestPriority', 'runDBTest', 'DBTESTDEF_extensions', 'DBTESTDEF_settings'):
   globals()[k]=getattr(m, k)

sys.path.append('/var/python/libs/')
sys.path.append('/var/python/')
sys.path.append('/home/python/libs/')
sys.path.append('/home/python/')

from functionsex import *

class ScreendeskTestDB(object):
   def __init__(self, workspace):
      self.workspace=workspace
      path=getScriptPath(real=True, f=__file__)+'/tmp/db1'
      self.db=VombatiDB(DBTESTDEF_extensions)(self.workspace, path)
      for k, v in DBTESTDEF_settings.iteritems():
         self.db.settings[k]=v
      self.configNS()
      self.db.connect()

   def configNS(self):
      self.db.configureNS([
      #  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa
         ('tmp', (None, None, None, None, None, False)),
         ('user', (None, ['operator', 'project'], {'email':'str', 'name':'str'})),
         ('operator', (['user', 'project'], ['project'], {'__needed':False, '__allowUnknown':True, 'login':'str'}, True, True, ['user'])),
         ('project', (['user', 'operator'], ['operator'], {'url':'str', 'type':'str'}, True, True, ['user'], True)),
      ], andClear=True)

   def modifyProp(self, ids, data):
      for ids, (props, l) in self.db.iterBranch(ids, calcProperties=False):
         props.update(data)

   def show(self, branch=None):
      showDB(self.db, branch=branch)

   def _clearDB(self):
      self.db.truncate()
      self.configNS()

   def _makeLongLink(self, l=10, root=None, ns='_tmpLink'):
      if root is None:
         self.db.set('longLink_root', {'data':'root of long-link'}, strictMode=False, onlyIfExist=False)
         chain=('longLink_root',)
      else:
         chain=root if isTuple(root) else (root,)
      for i in xrange(l):
         k='%s%s'%(ns, i)
         self.db.link(chain+(k,), chain, strictMode=False, onlyIfExist=False)
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
      root='ll_ROOT'+randomEx()+randomEx()+randomEx()
      self.db.set(root, {'k1':'test', 'k2':314})
      try:
         ids=self._makeLongLink(root=root, l=lll, ns='ll_item')
         self.db.setNS('ll_item', columns={'k1':'str', 'k2':'int'})
         timeitMe(lambda: self.db.set(root, {'k1':str(time.time()), 'k2':int(time.time())}), n=100, m=1)
      finally:
         self.db.remove(root)
         self.db.delNS('ll_item', strictMode=False, allowCheckIndex=False)
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

   def searchTest(self):
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

   def loadTree(self, tree):
      loadTree(self.db, tree)

   def dump(self, branch=None, tree=NULL):
      if tree is not NULL:
         return dumpTree(self.db, tree)
      else:
         return dumpDB(self.db, branch=branch)

   def diff(self, dmpWith, branch=None):
      if self.dump(branch=branch)==dmpWith: return False
      print 'DUMPS MISMATCH!'
      for msg, ids, data in diffDB(self.db, dmpWith, branch=branch, checkMeta=False, checkProps=False):
         print '   %s for %s: %r'%(msg, ids, data)
      return True

   def moveDataTest(self):
      print '-'*25, 'Move data test', '-'*25
      TEST_ROOT='TEST_moveData'
      try:
         self.db.setNS(TEST_ROOT, None, None, onlyIndexed=False, linkChilds=True, allowCheckIndex=True)
         self.db.setNS('mv_root', None, None, onlyIndexed=False, linkChilds=True, allowCheckIndex=True)
         self.db.setNS('mv_target', None, None, onlyIndexed=False, linkChilds=True, allowCheckIndex=True)
         self.db.setNS('mv_link', None, None, onlyIndexed=False, linkChilds=True, allowCheckIndex=True)
         self.db.remove(TEST_ROOT)
         treeBefore={TEST_ROOT:{
            ('mv_root#1', None):{
               ('mv_target#1old', None):None,
               ('mv_target#2old', None):{
                  ('mv_target#2node1', None):None,
                  ('mv_target#2node2', None):{
                     ('mv_target#2node3', None):None
                  },
               },
               ('mv_target#3old', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                  ('mv_target#3node1', None):{
                     ('mv_target#3node2', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                        ('mv_target#3node3', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):None
                     }
                  }
               },
               ('mv_target#4', None):None,
               ('mv_link#1', (TEST_ROOT, 'mv_root#1', 'mv_target#1old')):{
                  ('mv_link#2', (TEST_ROOT, 'mv_root#1', 'mv_target#1old')):None,
               },
               ('mv_link#4', (TEST_ROOT, 'mv_root#1', 'mv_target#2old')):None,
               ('mv_link#5', (TEST_ROOT, 'mv_root#1', 'mv_target#2old', 'mv_target#2node1')):None,
               ('mv_link#6', (TEST_ROOT, 'mv_root#1', 'mv_target#2old', 'mv_target#2node2')):None,
               ('mv_target#5old', None):{
                  ('mv_target#6old', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                     ('mv_target#7old', None):None,
                  },
               },
            },
            ('mv_root#2', None):{
               ('mv_link#3', (TEST_ROOT, 'mv_root#1', 'mv_target#1old')):None,
               ('mv_link#7', (TEST_ROOT, 'mv_root#1', 'mv_target#2old', 'mv_target#2node2', 'mv_target#2node3')):None,
            },
            ('mv_root#3', None):None,
         }}
         self.loadTree(treeBefore)
         assert not self.diff(self.dump(tree=treeBefore), branch=TEST_ROOT)
         #
         self.db.move((TEST_ROOT, 'mv_root#1', 'mv_target#1old'), (TEST_ROOT, 'mv_root#3', 'mv_target#1new'), strictMode=False)
         self.db.move((TEST_ROOT, 'mv_root#1', 'mv_target#2old'), (TEST_ROOT, 'mv_root#3', 'mv_target#2new'), strictMode=False)
         self.db.move((TEST_ROOT, 'mv_root#1', 'mv_target#3old'), (TEST_ROOT, 'mv_root#3', 'mv_target#3new'), strictMode=False)
         self.db.move((TEST_ROOT, 'mv_root#1', 'mv_target#5old'), (TEST_ROOT, 'mv_root#3', 'mv_target#5new'), strictMode=False)
         #
         treeAfter={TEST_ROOT:{
            ('mv_root#1', None):{
               ('mv_target#4', None):None,
               ('mv_link#1', (TEST_ROOT, 'mv_root#3', 'mv_target#1new')):{
                  ('mv_link#2', (TEST_ROOT, 'mv_root#3', 'mv_target#1new')):None,
               },
               ('mv_link#4', (TEST_ROOT, 'mv_root#3', 'mv_target#2new')):None,
               ('mv_link#5', (TEST_ROOT, 'mv_root#3', 'mv_target#2new', 'mv_target#2node1')):None,
               ('mv_link#6', (TEST_ROOT, 'mv_root#3', 'mv_target#2new', 'mv_target#2node2')):None,
            },
            ('mv_root#2', None):{
               ('mv_link#3', (TEST_ROOT, 'mv_root#3', 'mv_target#1new')):None,
               ('mv_link#7', (TEST_ROOT, 'mv_root#3', 'mv_target#2new', 'mv_target#2node2', 'mv_target#2node3')):None,
            },
            ('mv_root#3', None):{
               ('mv_target#1new', None):None,
               ('mv_target#2new', None):{
                  ('mv_target#2node1', None):None,
                  ('mv_target#2node2', None):{
                     ('mv_target#2node3', None):None
                  },
               },
               ('mv_target#3new', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                  ('mv_target#3node1', None):{
                     ('mv_target#3node2', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                        ('mv_target#3node3', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):None
                     }
                  }
               },
               ('mv_target#5new', None):{
                  ('mv_target#6old', (TEST_ROOT, 'mv_root#1', 'mv_target#4')):{
                     ('mv_target#7old', None):None,
                  },
               },
            },
         }}
         assert not self.diff(self.dump(tree=treeAfter), branch=TEST_ROOT)
         self.show(TEST_ROOT)
      except Exception:
         self.show(TEST_ROOT)
         raise
      finally:
         self.db.remove(TEST_ROOT)
         self.db.delNS(TEST_ROOT, strictMode=True, allowCheckIndex=True)
         self.db.delNS('mv_root', strictMode=True, allowCheckIndex=True)
         self.db.delNS('mv_target', strictMode=True, allowCheckIndex=True)
         self.db.delNS('mv_link', strictMode=True, allowCheckIndex=True)
      print 'OK!'

   def test(self):
      # self.modifyProp(None, {'branchModified':False})
      self.show()
      if self.workspace.raw_input('Clear DB? ')=='y': self._clearDB()
      if self.workspace.raw_input('Fill DB with new random data? ')=='y':
         _cUsers=intEx(self.workspace.raw_input('Max users: '))
         _cProjs=intEx(self.workspace.raw_input('Max projects: '))
         _cOps=intEx(self.workspace.raw_input('Max operators: '))
         self._fillDB(_cUsers, _cProjs, _cOps)
      if self.workspace.raw_input('Create long chain of links? ')=='y':
         l=intEx(self.workspace.raw_input("Chain's length: "))
         self._makeLongLink(l)
      if self.workspace.raw_input('Run speed-tests? ')=='y':
         self.speedtestWrite()
         self.speedtestRead()

      # timeitMe(lambda:self.db.get(('root', 'k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9')), n=1000, m=1)
      # self.db._speedStats.clear()
      # f=bind(self.db.iterBranch, {'ids':None, 'recursive':True, 'treeMode':True, 'safeMode':True, 'offsetLast':False, 'calcProperties':True})
      # timeitMe(lambda f=f: tuple(f()), n=1000, m=1)
      # showStats(self.db)
      # raw_input()

      if self.workspace.raw_input('Run search-test? ')=='y':
         self.searchTest()
      if self.workspace.raw_input('Run move-test? ')=='y':
         self.moveDataTest()

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

      if self.workspace.raw_input('Show data again? ')=='y': self.show()
      if console.inTerm() and self.workspace.raw_input('%(bgmagenta)s%(white)sRun interactive mode?%(end)s '%consoleColor)=='y':
         console.interact(scope=locals())
      showStats(self.db)

if __name__ == '__main__':
   ErrorHandler()
   workspace=Workspace()
   # start testing
   ScreendeskTestDB(workspace).test()
