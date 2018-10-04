# -*- coding: utf-8 -*-

class BaseDBError(Exception):
   __slots__=()
   pass

class BaseDBErrorPrefixed(BaseDBError):
   """Something goes wrong in VambatiDB"""
   __slots__=('msg',)
   def __init__(self, msg=None, useDefPrefix=True):
      if msg and useDefPrefix:
         msg='%s: %s'%(self.__class__.__doc__, msg)
      else:
         msg=msg or self.__class__.__doc__
      super(BaseDBError, self).__init__(msg)

class BadIdError(BaseDBErrorPrefixed):
   """Object's id contains forbidden symbols"""

class BadLinkError(BaseDBErrorPrefixed):
   """Referring to non-existed object"""

class StrictModeError(BaseDBError):
   """Raise this only in strict mode for cases, where usually we shows warning"""

class ParentNotExistError(BaseDBErrorPrefixed, StrictModeError):
   """Parent not exists"""

class ExistStatusMismatchError(BaseDBErrorPrefixed, StrictModeError):
   """Exist-status mismatching"""

class NotExistError(BaseDBErrorPrefixed, StrictModeError):
   """Object not exists"""

class ExtensionDependencyError(BaseDBErrorPrefixed):
   """Unmet dependencies for extension"""
