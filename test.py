def extractPrice(line):
    temp = line.split(",")
    return ((temp[0],temp[1]))

s ="44,8602,37.19"

print(extractPrice(s))