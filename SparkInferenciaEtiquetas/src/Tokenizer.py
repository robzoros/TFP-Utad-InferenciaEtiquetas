#!/usr/bin/env python
from nltk.tokenize.moses import MosesTokenizer, MosesDetokenizer
import sys
import inflect
p = inflect.engine()

for line in sys.stdin:
  print " ".join(map(lambda word: p.singular_noun(word).encode('utf-8') if  p.singular_noun(word) else word.encode('utf-8'), MosesTokenizer().tokenize(line.decode('utf-8'))))