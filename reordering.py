from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import RawProtocol
from operator import itemgetter
import numpy as np

class MRJob_unorderedTotalOrderSort(MRJob):

    # Allows values to be treated as keys, so they can be used for sorting:
    MRJob.SORT_VALUES = True

    # The protocols are critical. It will not work without these:
    INTERNAL_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def __init__(self, *args, **kwargs):
        super(MRJob_unorderedTotalOrderSort, self).__init__(*args, **kwargs)
        self.NUM_REDUCERS = 3

    def mapper(self, _, line):
        line = line.strip()
        key,value = line.split('\t')
        if int(key) > 20:
            yield "A",key+"\t"+value
        elif int(key) > 10:
            yield "B",key+"\t"+value
        else:
            yield "C",key+"\t"+value

    def reducer(self,key,value):
        for v in value:
            yield key, v

    def steps(self):

        JOBCONF_STEP1 = {
            'stream.num.map.output.key.fields':3,
            'stream.map.output.field.separator':"\t",
            'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class': 'org.apache.Hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2nr -k3,3',
            'mapred.reduce.tasks': self.NUM_REDUCERS,
            'partitioner':'org.apache.Hadoop.mapred.lib.KeyFieldBasedPartitioner'
        }
        return [MRStep(jobconf=JOBCONF_STEP1,
                    mapper=self.mapper,
                    reducer=self.reducer)
                ]

if __name__ == '__main__':
    MRJob_unorderedTotalOrderSort.run()
