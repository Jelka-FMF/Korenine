from time import sleep
print("#" + "00ff00"*1000, flush=True)
sleep(1)
for i in range(10):
    print("#" + ''.join("ffffff" if (j >> i) & 1 else "000000" for j in range(1000)), flush=True)
    sleep(1)

for i in reversed(range(10)):
    print("#" + ''.join("ffffff" if (j >> i) & 1 else "000000" for j in range(1000)), flush=True)
    sleep(1)

