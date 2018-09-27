#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os

if __name__=='__main__':
   mainPath=os.path.dirname(os.path.realpath(__file__))
   os.system(sys.executable+' '+mainPath+'/testSuite/__init__.py')
