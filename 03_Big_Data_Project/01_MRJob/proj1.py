from mrjob.job import MRJob
from mrjob.step import MRStep
import re


# ---------------------------------!!! Attention Please!!!------------------------------------
# Please add more details to the comments for each function. Clarifying the input
# and the output format would be better. It's helpful for tutors to review your code.

# Using multiple MRSteps is permitted, please name your functions properly for readability.

# We will test your code with the following comand:
# "python3 project1.py -r hadoop hdfs_input -o hdfs_output --jobconf mapreduce.job.reduces=2"

# Please make sure that your code can be compiled before submission.
# ---------------------------------!!! Attention Please!!!------------------------------------

class proj1(MRJob):

    # define your own mapreduce functions
    # --------------------------------------------------- Step1 ------------------------------------------------------------
    def mapper1(self, _, line):
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        if len(words):                                  # len(words) should equl to 3: user, loc, time
            yield words[0] + ",*", 1                    # the record number of check-ins from uj + 1                            e.g. "u1,*"  1
            yield words[0] + "," + words[1], 1          # the record number of check-ins at location loci by user 𝑢j + 1.       e.g. "u1,l1"  1

    def combiner1(self, key, values):
        yield key, sum(values)                          # get sum for the two parameter

    def reducer_init1(self):
        self.n_uj = 0                                   # set a placeholder to record the denominator number

    def reducer1(self, key, values):
        uj, loci = key.split(",", 1)
        if loci == "*":
            self.n_uj = sum(values)                     # if *  =>  record the denominator number
        else:
            count = sum(values)                         # if not * => calculate and yield
            yield None,loci+','+uj+','+str(count/self.n_uj)              # e.g. ""	    "l1,u1,0.6666666666666666"

    # -------------------------------------------------- Step2 -------------------------------------------------------------
    def mapper2(self,_,line):                           # ignore the key as '_'
        l1 = 'a,' + line                                # deal with the bug mentioned in lab4
        yield l1, None                                  # e.g. "a,loci, uj, prob"   ""

    def reducer2(self, key, values):
        a,loci,uj,prob = key.split(',')
        yield loci,uj+','+prob                            # e.g.  "loci"    "uj+prob"


    # --------------------------------------------------- Steps ------------------------------------------------------------
    def steps(self):
        SORT_VALUES = True

        JOBCONF = {
            # jobconf acts on mapper, so -k1,1:'a', -k2,2:'loci', -k3,3:'uj', -k4,4:'prob'
            # Sort the loci first, then flashback the prob, and finally sort the uj.
            'mapreduce.map.output.key.field.separator': ',',
            'mapreduce.partition.keypartitioner.options': '-k1,2',
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2 -k4,4nr -k3,3'
        }

        return [
            MRStep(mapper=self.mapper1,
                   combiner=self.combiner1,
                   reducer_init=self.reducer_init1,
                   reducer=self.reducer1),
            MRStep(jobconf=JOBCONF,
                   mapper=self.mapper2,
                   reducer=self.reducer2)
        ]


if __name__ == '__main__':
    proj1.run()