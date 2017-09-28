def logsim(nstud,notp,skills,noise=None):
    """
    Returns a list of lists of simulated student data.
    
    nstud:  int, the number of students per skill to simulate
    notp:   int, the number of problems to simulate per student per skill
    skills: dict, skill names as keys and bkt parameters as list for values
            e.g. {'skill1': [l0,g,s,t],'skill2': [l0,g,s,t]}
    noise:  float, default None. Used to add noise to correctness predictions.
            loggen_bkt predicts student correctness, and compares that value
            to a flat random distribution between 0 and 1. If noise is set to 
            a float, it is passed as the standard deviation to a gaussian 
            centered to zero, and added to the result of the flat random.
            noise = -1 is true random data.
    """
    import random
    
    class _skill:
        def __init__(self,s):
            self.l0 = s[0]
            self.g = s[1]
            self.s = s[2]
            self.t = s[3]
            return 
        
    def _ln(skill,ln,correct):
        mns = ln*(1-skill.s)
        ms = ln*skill.s 
        nmg = (1-ln)*skill.g
        nc = 1-correct
        nmng = (1-ln)*(1-skill.g)
        
        ca = float(mns)/float((mns+nmg))
        ica = float(ms)/float((ms+nmng))
        new = ((correct*ca) + (nc*ica))
        
        return new + ((1-new)*skill.t)
    
    i = 1
    
    r = [["index","studentid","skillid","correct"]]
    for _s in skills.keys():
        skill = _skill(skills[_s])
        for j in xrange(nstud):
            ln = skill.l0
            for _ in xrange(notp):
                out = [i,j,_s]
                pcorr = (ln * (1 - skill.s)) + ((1 - ln) * skill.g)
                
                if noise == -1:
                    pcorr = random.choice([0,1])
                    ln = _ln(skill,ln,pcorr)
                    out.append(pcorr)
                    r.append(out)
                    i += 1
                    continue
                
                if noise:
                    c = random.random() + random.gauss(0,noise)
                    if c < 0: c = 0
                    if c > 1: c = 1
                    
                else:
                    c = random.random()
                    
                ln = _ln(skill,ln,1) if pcorr > c else _ln(skill,ln,0)
                out.append(1 if pcorr > c else 0)
                r.append(out)
                i += 1
    return r

if __name__ == "__main__":
    import csv
    import os
    
    fdir = "your output directory"
    fname = "your output filename"
    
    nstud = 15
    notp = 5
    skills = {'skill1':[0.15,0.1,0.1,0.05],
              'skill2':[0.35,0.15,0.05,0.15]}
    noise = 0.5
    
    res = logsim(nstud,notp,skills,noise)
    
    os.chdir(fdir)
    writer = csv.writer(open(fname+".csv",'wb'))
    writer.writerows(res)


'''
Created on Jan 28, 2017
v1.0 released Sep 22, 2017
Correspondence and bugs should be directed to slater.research@gmail.com

Licensed under the MIT License (MIT) - copyright 2017
'''
