#!/usr/bin/env python
import sys
import langid

for line in sys.stdin:
  tupla = langid.classify(line)
  print tupla[0]