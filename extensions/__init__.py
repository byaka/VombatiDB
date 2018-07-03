# -*- coding: utf-8 -*-

from os.path import dirname, basename, splitext, isfile, join
from os import listdir

path=dirname(__file__)
extensions={}
for f in listdir(path):
    fp=join(path, f)
    fn, fe=splitext(basename(f))
    if isfile(fp):
        if fe!='.py': continue
        if fn.startswith('_'): continue
        extensions[fn]=__import__(fn, globals(), locals())
    else: pass  #! add support for importing dirs
