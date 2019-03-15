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

class BaseDBError(Exception):
   __slots__=()
   pass

class NotSupportedError(BaseDBError):
   """This ability not supported"""

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
