class hartsynch:
    def __init__(self,tdiff=None,hartloc=None,tutorloc=None,df_hart=None,
                 df_tutor=None,hart_time_header=None,hart_student_header=None,
                 tutor_time_header=None,tutor_student_header=None):
        from os import listdir
        from os.path import isdir,isfile,join
        
        self.tdiff = tdiff
        self.hd = df_hart
        self.td = df_tutor
        self.ht = hart_time_header
        self.hs = hart_student_header
        self.tt = tutor_time_header
        self.ts = tutor_student_header
        self.hartloc = hartloc 
        self.tutorloc = tutorloc
        
        if isdir(hartloc):
            self.hartfiles = [f for f in listdir(hartloc) if isfile(join(hartloc,f))]
            if ".DS_Store" in self.hartfiles:
                self.hartfiles.remove(".DS_Store")
        else:
            self.hartfiles = [hartloc]
        self.join_hart_logs()
            
        if isdir(tutorloc):
            self.tutorfiles = [f for f in listdir(tutorloc) if isfile(join(tutorloc,f))]
            if ".DS_Store" in self.tutorfiles:
                self.tutorfiles.remove(".DS_Store")      
        else:
            self.tutorfiles = [tutorloc]
        self.join_tutor_logs()
    
    def unix_to_datetime(self,timestamp,d_format):
        '''
        Converts UNIX timestamps to more easily readable datetime strings.
        '''
        from datetime import datetime
        return datetime.fromtimestamp(int(timestamp)).strftime(d_format)
    
    def datetime_to_unix(self,timestamp,d_format):
        '''
        Converts datetime strings to UNIX timestamps.
        For adjusting the offset of non-GMT datetimes, use adjust_offset.
        '''
        from datetime import datetime
        d =  datetime.strptime(timestamp,d_format)
        return (d-datetime(1970,1,1)).total_seconds()
    
    def join_hart_logs(self):
        '''
        This function aggregates several separate HART logs into one single
        list of lists object within Python.
        '''
        import csv
        import os
        
        self.hartinfo = {}
        self.hartlogs = None
        os.chdir(self.hartloc)
        i = 0
        for f in self.hartfiles:
            with open(f,'rU') as fobj:
                raws = list(csv.reader(fobj))
            self.hartinfo[f] = dict(zip(raws[0][1:],raws[1]))
            self.hartinfo[f]["data"] = raws[3:]
            self.hartheader = raws[2][1:]
            self.hartheader.append("Coder")
            self.hartheader.append("UniqueID")
            self.hartheader.append("Filename")
            self.adjust_offset(f)
            
            for n,x in enumerate(self.hartinfo[f]["data"]):
                x.append(self.hartinfo[f]["username"])
                x.append(str(n)+str(x[self.hartheader.index(self.ht)]))
                x.append(f)
                
        self.hartdata = [self.hartinfo[key]["data"] for key in self.hartinfo]
        self.hartobs = [item for sublist in self.hartdata for item in sublist]
    
    def join_tutor_logs(self):
        '''
        This function aggregates several separate tutor files into one single
        list of lists object in Python. It's not necessary for this particular
        project, but I wrote it for the future.
        '''
        import csv
        import os
        
        os.chdir(self.tutorloc)
        
        self.tutorobs = []
        self.tutorheader = None
        
        for x in self.tutorfiles:
            with open(x,"rU") as tutorf:
                reader = csv.reader(tutorf)
                if not self.tutorheader:
                    self.tutorheader = reader.next()
                filedata = list(reader)
                self.tutorobs.extend(filedata)
        os.chdir("..")
        return
    
    def adjust_offset(self,hartfile):
        '''
        This function is internal to join_hart_logs, you shouldn't be calling
        it alone.
        
        As of the time of this code, HART files provide a datetime object epoch,
        with millisecond-offset timestamps following that. This function
        serves two purposes - standardize these time objects, and also bring
        them into line with the timezone of the tutor program.
        
        tdiff is a required argument, and should be set to the difference
        in timezones between hart observer and tutor data, in seconds. This
        code does not use robust timezones, because timezones are dumb and it's
        research code. It just makes the two times agree with one another.
        
        A handy chart of time conversions:
        1h = 3600s
        30m = 1800s
        15m = 900s
        10m = 600s
        1m = 60s
        '''

        epoch = int(self.hartinfo[hartfile]['ntptimestamp_ms'][-13:])
#         epoch = epoch + 18000000
        for item in self.hartinfo[hartfile]["data"]:
            offset = int(item[self.hartheader.index(self.ht)])
            item[self.hartheader.index(self.ht)] = epoch + offset
        return
    
    def correlate(self):
        '''
        Sort both lists by student and time, set a positional index of zero for 
        each, and iterate through both lists. Computes each student separately 
        so that I don't have to worry about exception handling at points where 
        you transition from one student to another.
        
        Because we want to preserve unmatched rows, we make "blank" observation
        rows and then replace those rows with the relevant data depending on
        where the positional indices wind up in relation to one another.
        
        After the loop executes for each positional index, we write to a .csv 
        file according to the conditions that we found based on the timestamps
        associated with the positional indices in each file.
        '''
        import csv 
        from operator import itemgetter
        import copy
        
        writer = csv.writer(open("20170815 Synchronization Output Logs.csv","wb"))
        outputheader = self.hartheader + self.tutorheader
        
        writer.writerow(outputheader)
        
        self.hartobs = sorted(self.hartobs,key=itemgetter(self.hartheader.index(self.hs),self.hartheader.index(self.ht)))
        self.tutorobs = sorted(self.tutorobs,key=itemgetter(self.tutorheader.index(self.ts),self.tutorheader.index(self.tt)))
        
        self.ind_ht = self.hartheader.index(self.ht)
        self.ind_hs = self.hartheader.index(self.hs)
        self.ind_tt = self.tutorheader.index(self.tt)
        self.ind_ts = self.tutorheader.index(self.ts)
        
        hart_blank = [""]*len(self.hartobs[0])
        tutor_blank = [""]*len(self.tutorobs[0])
        
        list_of_students = list(set([x[0] for x in self.hartobs]))
        list_of_students.remove("A81")
        total_matches = 0
        no_corr = 0

        for s in list_of_students:
            self.hartobs_s = filter(lambda x: x[self.hartheader.index(self.hs)]==s,self.hartobs)
            self.tutorobs_s = filter(lambda x: x[self.tutorheader.index(self.ts)]==s,self.tutorobs)
            
            idx_h = 0
            idx_t = 0
            
            matches = 0
            
            hart_rows_written = []
            tutor_rows_written = []
            
            actions_used = []
        
            while idx_h <= len(self.hartobs_s)-1 and idx_t <= len(self.tutorobs_s)-1:
                obs_gap = int(self.hartobs_s[idx_h][self.ind_ht]) - int(self.tutorobs_s[idx_t][self.ind_tt])
                
                idx_t_orig = copy.copy(idx_t)
                
                if idx_h >= len(self.hartobs_s)-1:
                    if self.hartobs_s[idx_h] != self.hartobs_s[-1]:
                        print "Something's gone wrong - indices don't agree."
                        quit()
                    ## Maxed out our HART observations. There's nothing more to be
                    ## correlated. Writing the rest of the tutor logs.
                    
                    while idx_t <= len(self.tutorobs_s)-1:
                        event = hart_blank + self.tutorobs_s[idx_t]
                        writer.writerow(event)
                        idx_t += 1
                        
                        actions_used.append(event[10])
                    break
                    
                elif idx_t_orig >= len(self.tutorobs_s)-1:
                    if self.tutorobs_s[idx_t_orig] != self.tutorobs_s[-1]:
                        print "Something's gone wrong - indices don't agree."
                        quit()
                    ## Maxed out our tutor observations. There's nothing more to be
                    ## correlated. Writing the rest of the HART logs.
                    
                    while idx_h <= len(self.hartobs_s)-1:
                        event = self.hartobs_s[idx_h] + tutor_blank
                        writer.writerow(event)
                        idx_h += 1
                        
                        actions_used.append(event[10])
                    break
                
                if obs_gap < 3000:
                    # the HART index is too far behind and needs to advance.
                    # there's no tutor observation associated with this HART observation.
                    # write the HART row with blank tutor data.
                    if idx_h not in hart_rows_written:
                        event = self.hartobs_s[idx_h] + tutor_blank
                        writer.writerow(event)
                        hart_rows_written.append(idx_h)
                        
                        actions_used.append(event[10])
                    idx_h += 1
                    continue
                
                if obs_gap > 23000:
                    # the HART index is too far ahead and needs to wait
                    # we don't yet know if the HART observation pertains to a tutor observation
                    # write the tutor row
                    if idx_t not in tutor_rows_written:
                        event = hart_blank + self.tutorobs_s[idx_t]
                        writer.writerow(event)
                        tutor_rows_written.append(idx_t)
                        
                        actions_used.append(event[10])
                    idx_t += 1
                    continue
                    
                if 3000 <= obs_gap <= 23000:
                    # the HART index found a matching tutor timestamp
                    # this HART observation is linked to the tutor observation
                    # write the combined row
                    
                    matches += 1
                    event = self.hartobs_s[idx_h] + self.tutorobs_s[idx_t]
                    writer.writerow(event)
                    hart_rows_written.append(idx_h)
                    tutor_rows_written.append(idx_t)
                    idx_t += 1
                    
                    actions_used.append(event[10])
                    continue
                
            if matches == 0:
                no_corr += 1
            total_matches += matches
        return
    
if __name__ == "__main__":
    import os
    import csv
    hartdir = "/Users/stefanslater/Desktop/BB Data/BROMP LOGS USE THESE"
    tutordir = "/Users/stefanslater/Desktop/BB Data/TUTOR LOGS USE THESE"
    hart = hartsynch(tdiff=5,hartloc=hartdir,tutorloc=tutordir,
                     df_hart="%m.%d.%Y at %H:%M:%S",
                     df_tutor="%Y-%m-%d %H:%M:%S",
                     hart_time_header="msoffsetfromstart",
                     hart_student_header="studentid",
                     tutor_time_header="start_time",
                     tutor_student_header="subject_id")

    hart.correlate()
    
    with open("20170815 Synchronization Output Logs.csv",'rU') as output:
        reader = csv.reader(output)
        print len(list(reader))
'''
Created on Jun 3, 2017

@author: stefanslater
'''
