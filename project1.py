from mrjob.job import MRJob
from mrjob.step import MRStep


class CheckinProb(MRJob):
    
    def mapper(self, _, line):
        u, l, t = line.strip().split(",")
        yield u+","+l, 1
        #emit Nuj
        yield u+",*", 1
    
    def reducer_init(self):
        self.user_total_checkin = 0
        
    def reducer(self, key, values):
        u, l = key.split(",")
        #sum Nuj first
        if l == "*":
            self.user_total_checkin = sum(values)
        #then count prob
        else:
            count = sum(values)
            prob = count/self.user_total_checkin
            yield l+","+u+","+str(prob), ""
    
    #second step to sort the ouput to desired format
    def reducer2(self,key,values):
        l,u,prob = key.split(",")
        yield l,u+","+prob
    
    #set this to TRUE, then reducers can receive the values associated 
    #with any key in sorted order
    SORT_VALUES = True
    
    
    def steps(self):
        JOBCONF1 = {
            'mapreduce.map.output.key.field.separator':',',
            #key is u,l, so reducer need to receive via same u
            'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #key is u,l, there is * in l, so need to sort l sencondarily
            'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2'
        }
        JOBCONF2 = {
            'mapreduce.map.output.key.field.separator':',',
            #key is l,u,prob, so need same l in same reducer
            'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #key is l,u,prob, so first sort l, then prob in descending, then u
            'mapreduce.partition.keycomparator.options': '-k1,1 -k3,3nr -k2,2'
        }
        return [
            MRStep(
                jobconf = JOBCONF1,
                mapper = self.mapper,
                reducer_init = self.reducer_init,
                reducer = self.reducer
            ),
            MRStep(
                jobconf = JOBCONF2,
                reducer = self.reducer2
            )
        ]
    
if __name__ == '__main__':
    CheckinProb.run()
