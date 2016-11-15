from matplotlib import pyplot

x1 = []
# capacity
y1 = []
# ops
y2 = []
# bytes
y3 = []
# total
y4 = []
# cloud ops
y5 = []
y5Labels = []
start = 0
ops = 0
with open('s3server.log') as f:
    for line in f.read().splitlines():
        ops += 1
        parts = line.split(' ', 5)
        if not x1:
            start = int(parts[0])
        x1.append(int(parts[0]) - start)
        y1.append(float(parts[4]) / 1024 / 1024 * 0.03)
        y2.append(ops * 0.01)
        y3.append(float(parts[2]) / 1024 / 1024 * 0.09)
        y4.append(y1[-1] + y2[-1] + y3[-1])
        try:
            index = y5Labels.index(parts[5])
            y5.append(index + 1)
        except:
            y5Labels.append(parts[5])
            y5.append(len(y5Labels))

pyplot.clf()

ax1 = pyplot.subplot(511)
pyplot.plot(x1, y4, '-')
pyplot.xlabel('time')
pyplot.ylabel('total cost')

ax2 = pyplot.subplot(512, sharex=ax1)
pyplot.plot(x1, y1, '-')
pyplot.xlabel('time')
pyplot.ylabel('capacity cost')

ax3 = pyplot.subplot(513, sharex=ax1)
pyplot.plot(x1, y2, '-')
pyplot.xlabel('time')
pyplot.ylabel('ops cost')

ax4 = pyplot.subplot(514, sharex=ax1)
pyplot.plot(x1, y3, '-')
pyplot.xlabel('time')
pyplot.ylabel('bytes read cost')

ax5 = pyplot.subplot(515, sharex=ax1)
pyplot.xlabel('time')
pyplot.yticks(range(1, len(y5Labels) + 1), y5Labels)
pyplot.plot(x1, y5, '.')

pyplot.tight_layout()
fig = pyplot.gcf()
fig.set_size_inches(14.5, 19.5)
pyplot.savefig('graph.png')
