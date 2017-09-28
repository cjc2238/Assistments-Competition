import csv
import os

os.chdir("/Users/stefanslater/Desktop/Reasoning Mind")

with open("Pre-survey AY 16-17.csv","rU") as f:
    d = list(csv.reader(f))
    
# removing students who failed to answer all questions.

droprows = []
for n,li in enumerate(d):
    if "NA" in li[3:]:
        droprows.append(n)
        
droprows.sort(reverse=True)

for i in droprows:
    del d[i] 

# removing questions that we are no longer using: 6, 13, 14
dropcols = ['Q6','Q13','Q14','Grade','Timestamp']
newd = []
writer = csv.writer(open("20170915_ReasMindLCAInput.csv","wb"))
for li in d[1:]:
    newd.append([x for n,x in enumerate(li) if d[0][n] not in dropcols])
    
for li in newd:
    for n,x in enumerate(li[1:]):
        if int(x) >= 3:
            li[n+1] = 1
        else:
            li[n+1] = 0
    writer.writerow(li)