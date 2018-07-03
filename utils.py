# -*- coding: utf-8 -*-
import sys
global PY_V
PY_V=float(sys.version[:3])

try:
   import cPickle as pickle
except ImportError:
   import pickle

sys.path.append('/var/python/libs/')
sys.path.append('/var/python/')
sys.path.append('/home/python/libs/')
sys.path.append('/home/python/')
from functionsex import *
timetime=time.time

class BaseDBError(Exception):
   """ Base Exception class fro other DB-specific errors. """

class BadLinkError(BaseDBError):
   """ Raise this if link referred to non-exists data. """

class StrictModeError(BaseDBError):
   """ Raise this only in strict mode for cases, where usually we shows warning. """

class ExtensionDependencyError(BaseDBError):
   """ Raise this if requested extensions have unmet dependencies. """
