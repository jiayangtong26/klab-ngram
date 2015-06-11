import mrjob
from mrjob.job import MRJob
import ntpath
import nltk
import re
import sys
import os


class MRNGram(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    progPunctuation = re.compile("[^\w-]")
    progContainsALetterOrNumber = re.compile("[a-zA-Z0-9]")

    def mapper(self, _, line):
        
        N = 4

        filename = mrjob.compat.jobconf_from_env('map.input.file')
        filename = ntpath.basename(filename)
        # currently, the file name is like "595F_1852_01_01_0102.txt"
        # I just get rid of the last page number part "_0102.txt" 
        # to obtain the filename "595F_1852_01_01"
        fname = filename[:filename.rfind("_")]

        text = self.progPunctuation.sub(' ', line)
        tokens = text.split()
        toks = [w for w in tokens if self.progContainsALetterOrNumber.search(w)]

        #d = {}
        #for n in range(1, N+1):
        #    d[n] = {}

        for n in range(1, N+1):
            d = {}
            for ng in nltk.ngrams(toks, n):
                ngram = " ".join(ng)
                if ngram in d:
                    d[ngram] += 1
                else:
                    d[ngram] = 1
            # pickle
            for w, freq in d.items():
                yield (fname, n), (w, freq)
        

    def reducer(self, k, values):

        fname, n = k
        d = {}
        for w, freq in values:
            if w in d:
                d[w] += freq
            else:
                d[w] = freq
        res = []
        for w, freq in d.items():
            res.append(w+'\t'+str(freq))
        yield fname+"\t"+str(n)+"\t"+"\t".join(res),None



if __name__ == '__main__':
    MRNGram.run()