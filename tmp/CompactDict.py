#!/usr/bin/env python

# Copyright 2010 Lukasz Bolikowski. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice, this list of
#       conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright notice, this list
#       of conditions and the following disclaimer in the documentation and/or other materials
#       provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY LUKASZ BOLIKOWSKI ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LUKASZ BOLIKOWSKI OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""This module contains various memory-optimized data structures."""

import array

class CollisionError(Exception):
   pass

class IntSet:
   """A set of integers.  Implemented using a dictionary of integer arrays."""

   BLOCKBITS = 16

   def __init__(self, iterable = []):
      """Initializes the set.  Examples:
      >>> s = IntSet()
      >>> s = IntSet([0, 1, 8])
      >>> s = IntSet(set([0, 1, 8]))
      """
      self.__blocks = {}
      for item in iterable:
         self.add(item)

   def __getpos(self, elem):
      high, low = elem >> self.BLOCKBITS, elem % (1 << self.BLOCKBITS)
      if not high in self.__blocks:
         self.__blocks[high] = array.array('i')
      block = self.__blocks[high]
      n = len(block)
      l, r = 0, n
      while l < r:
         m = l + (r - l) // 2
         t = block[m]
         if t == elem:
            return high, m
         if t < elem:
            l = m + 1
         else:
            r = m
      return high, l

   def add(self, elem):
      """Adds an element to the set.  Examples:
      >>> s = IntSet([0, 1, 8])
      >>> len(s)
      3
      >>> s.add(8)
      >>> len(s)
      3
      >>> 27 in s
      False
      >>> s.add(27)
      >>> 27 in s
      True
      """
      b, p = self.__getpos(elem)
      block = self.__blocks[b]
      n = len(block)
      if p == n:
         block.append(elem)
      elif block[p] <> elem:
         block.insert(p, elem)

   def remove(self, elem):
      """Removes an element from the set.  Examples:
      >>> s = IntSet([0, 1, 8])
      >>> 0 in s
      True
      >>> s.remove(0)
      >>> 0 in s
      False
      >>> s.remove(-1)
      Traceback (most recent call last):
         ...
      KeyError: -1
      """
      b, p = self.__getpos(elem)
      block = self.__blocks[b]
      n = len(block)
      if p == n or block[p] <> elem:
         raise KeyError, elem
      block.pop(p)

   def __contains__(self, elem):
      """Tests whether an element is in the set.  Examples:
      >>> s = IntSet([0, 1, 8])
      >>> 0 in s
      True
      >>> -1 in s
      False
      """
      b, p = self.__getpos(elem)
      block = self.__blocks[b]
      n = len(block)
      if p == n:
         return False
      return block[p] == elem

   def __iter__(self):
      """Iterates over the elements of the set.  Examples:
      >>> s = IntSet([0, 1, 8])
      >>> t = 0
      >>> for e in s: t += e
      >>> t
      9
      >>> s = IntSet()
      >>> l = []
      >>> for i in xrange(2000): s.add(i*i)
      >>> for i in s: l += ([i] if i*i in s else [])
      >>> l == [i*i for i in range(45)]
      True
      """
      for b in self.__blocks.keys():
         block = self.__blocks[b]
         n = len(block)
         for p in xrange(n):
            yield block[p]

   def __len__(self):
      """Returns the number of elements in the set.  Examples:
      >>> s = IntSet([0, 1, 8])
      >>> len(s)
      3
      """
      result = 0
      for b in self.__blocks:
         result += len(self.__blocks[b])
      return result

   def __repr__(self):
      """Returns the "official" string representation of the set."""
      return 'IntSet(' + str(self) + ')'

   def __str__(self):
      """Returns an "informal" string representation of the set."""
      result, count = '', 0
      for elem in self:
         if count > 0:
            result += ', '
         if count == 100:
            result += '...'
            break
         result += str(elem)
         count += 1
      return '[' + result + ']'

class HashIntDict:
   """A dictionary-like structure with object hashes as keys
   and integers as values.
   """

   BLOCKBITS = 16

   CHK_IGNORING = 0
   CHK_DELETING = 1
   CHK_SHOUTING = 2

   def __init__(self, dictionary = {}, checking = 0):
      """Initializes the dictionary.  Examples:
      >>> d = HashIntDict()
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2}, HashIntDict.CHK_DELETING)
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2}, HashIntDict.CHK_SHOUTING)
      """
      bits = 8 * array.array('i').itemsize
      self.__masks = ((1 << bits) - 1, (1 << (bits - 1)) - 1)
      self.__blocks = {}
      self.__checking = checking
      if checking <> self.CHK_IGNORING:
         self.__collisions = IntSet()
      for key in dictionary:
         self[key] = dictionary[key]

   def __prepkey(self, key):
      if type(key) <> type(1):
         key = hash(key)
      key = key & self.__masks[0]
      if key > self.__masks[1]:
         key = key - self.__masks[0] - 1
      key = int(key)
      if self.__checking <> self.CHK_IGNORING and key in self.__collisions:
         if self.__checking == self.CHK_DELETING:
            return key, True
         else:
            raise CollisionError
      return key, False

   def intkey(self, key):
      """Converts the given key to internal key representation
      and checks for collision.

      Examples for a dictionary with CHK_IGNORING (default):
      >>> d = HashIntDict()
      >>> d.intkey(1)
      (1, False)
      >>> d.intkey(0)
      (0, False)
      >>> d.intkey(-1)
      (-1, False)

      Examples for a dictionary with CHK_DELETING:
      >>> d = HashIntDict({0: 0}, HashIntDict.CHK_DELETING)
      >>> d.intkey(0)
      (0, False)
      >>> d[0] = 0
      >>> d.intkey(0)
      (0, False)
      >>> d[0] = 1
      >>> d.intkey(0)
      (0, True)

      Examples for a dictionary with CHK_SHOUTING:
      >>> d = HashIntDict({0: 0}, HashIntDict.CHK_SHOUTING)
      >>> d.intkey(0)
      (0, False)
      >>> d[0] = 1
      Traceback (most recent call last):
         ...
      CollisionError
      >>> d.intkey(0)
      Traceback (most recent call last):
         ...
      CollisionError
      """
      return self.__prepkey(key)

   def __getpos(self, key):
      """Returns a block and a position within the block where
      the given key should be.  If a different key is present
      at the given position, or the position is out of the block's
      bounds, then it indicates that the key is not in the dictionary.

      A tuple with two integer values is returned.  The first value
      is the block's index.  The second value is the position within
      the block in zero-based "pair" units.  E.g., position 3 indicates
      the fourth pair (indices 6 and 7 in the array).
      """
      high, low = key >> self.BLOCKBITS, key % (1 << self.BLOCKBITS)
      if not high in self.__blocks:
         self.__blocks[high] = array.array('i')
      block = self.__blocks[high]
      n = len(block) // 2
      l, r = 0, n
      while l < r:
         m = l + (r - l) // 2
         t = block[2*m]
         if t == key:
            return high, m
         if t < key:
            l = m + 1
         else:
            r = m
      return high, l

   def get(self, key, default = None):
      try:
         return self[key]
      except KeyError:
         return default

   def iteritems(self):
      for key in self:
         yield key, self[key]

   def iterkeys(self):
      for key in self:
         yield key

   def itervalues(self):
      for key in self:
         yield self[key]

   def __contains__(self, key):
      """Tests whether a key is in the dictionary.  Examples:
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> 0 in d
      True
      >>> -1 in d
      False
      """
      key, collision = self.__prepkey(key)
      if collision:
         return False
      b, p = self.__getpos(key)
      block = self.__blocks[b]
      n = len(block) // 2
      if p == n:
         return False
      return block[2*p] == key

   def __delitem__(self, key):
      """Deletes a key from the dictionary.  Examples:
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> 0 in d
      True
      >>> del d[0]
      >>> 0 in d
      False
      >>> del d[-1]
      Traceback (most recent call last):
         ...
      KeyError: -1
      """
      key, _ = self.__prepkey(key)
      b, p = self.__getpos(key)
      block = self.__blocks[b]
      n = len(block) // 2
      if p == n or block[2*p] <> key:
         raise KeyError, key
      block.pop(2*p)
      block.pop(2*p)

   def __getitem__(self, key):
      """Gets a value from the dictionary.  Examples:
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> d[8]
      -2
      >>> del d[-1]
      Traceback (most recent call last):
         ...
      KeyError: -1
      """
      key, collision = self.__prepkey(key)
      if collision:
         return None
      b, p = self.__getpos(key)
      block = self.__blocks[b]
      n = len(block) // 2
      if p == n:
         raise KeyError, key
      if block[2*p] == key:
         return block[2*p + 1]
      raise KeyError, key

   def __iter__(self):
      """Iterates over the keys of the dictionary.  Examples:
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> s = 0
      >>> for k in d: s += d[k]
      >>> s
      -3
      >>> d = HashIntDict()
      >>> l = []
      >>> for i in xrange(2000): d[i] = i*i
      >>> for i in d: l += ([i] if i*i in d else [])
      >>> l == range(45)
      True
      """
      for b in self.__blocks.keys():
         block = self.__blocks[b]
         n = len(block) // 2
         for p in xrange(n):
            yield block[2*p]

   def __len__(self):
      """Returns the number of pairs in the dictionary.  Examples:
      >>> d = HashIntDict({0: 0, 1: -1, 8: -2})
      >>> len(d)
      3
      """
      result = 0
      for b in self.__blocks:
         result += len(self.__blocks[b]) // 2
      return result

   def __repr__(self):
      """Returns the "official" string representation of the dictionary."""
      return 'HashIntDict(' + str(self) + ')'

   def __setitem__(self, key, value):
      """Attempts to add a key-value pair to the dictionary.

      If the key was already present in the dictionary and
      collision checking was not set to CHK_IGNORING, then
      the current key-value pair is removed and a collision flag
      for the key is set.  Subsequent attempts to access the key
      or check for its presence will either behave as if the key
      did not exist, or CollisionError will be raised, depending
      on whether collision checking was set to CHK_DELETING or
      CHK_SHOUTING.

      Examples for a dictionary with CHK_IGNORING (default):
      >>> di = HashIntDict({0: 0, 1: -1, 8: -2, -4: 1, 'Foo': 1})
      >>> di[8] = -2
      >>> 8 in di
      True
      >>> di[1] = 1
      >>> 1 in di
      True

      Examples for a dictionary with CHK_DELETING:
      >>> dd = HashIntDict({0: 0, 1: -1, 8: -2}, HashIntDict.CHK_DELETING)
      >>> dd[8] = -2
      >>> 8 in dd
      True
      >>> dd[1] = 1
      >>> 1 in dd
      False
      >>> dd[1] = -1
      >>> 1 in dd
      False

      Examples for a dictionary with CHK_SHOUTING:
      >>> ds = HashIntDict({0: 0, 1: -1, 8: -2}, HashIntDict.CHK_SHOUTING)
      >>> ds[8] = -2
      >>> 8 in ds
      True
      >>> ds[1] = 1
      Traceback (most recent call last):
         ...
      CollisionError
      """
      key, collision = self.__prepkey(key)
      if collision:
         return
      b, p = self.__getpos(key)
      block = self.__blocks[b]
      n = len(block) // 2
      if p == n:
         block.append(key)
         block.append(value)
      elif block[2*p] == key:
         if self.__checking == self.CHK_IGNORING:
            block[2*p + 1] = value
         elif block[2*p + 1] <> value:
            self.__collisions.add(key)
            del self[key]
            if self.__checking == self.CHK_SHOUTING:
               raise CollisionError
      else:
         block.insert(2*p, value)
         block.insert(2*p, key)

   def __str__(self):
      """Returns an "informal" string representation of the dictionary."""
      result, count = '', 0
      for k in self:
         if count > 0:
            result += ', '
         if count == 100:
            result += '...'
            break
         result += str(k) + ': ' + str(self[k])
         count += 1
      return '{' + result + '}'

if __name__ == '__main__':
   import doctest
   doctest.testmod()

