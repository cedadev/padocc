import numpy as np

with open('../../tmp.txt') as f:
    lines = f.readlines()
ords = {'K':1,'M':1000,'G':1000000}
nums = []
for l in lines:
    intpart = True
    index = 0
    numpart = ''
    while intpart:
        char = l[index]
        if char in ords.keys():
            intpart = False
            mult = ords[char]
        else:
            numpart += char
        index += 1
    nums.append(float(numpart)*mult)

print(np.mean(nums), 'KB')