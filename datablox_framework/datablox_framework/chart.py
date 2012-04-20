import numpy as np
import numpy.numarray as na
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import json

with open("loads.json") as f:
  d = json.load(f)

#removing all group prefixes to save space
#TODO: only remove the unambiguous ones
labels = [i[i.rfind('.')+1:] for i in d.keys()]
full_labels = d.keys()
data =   d.values()
ticks = int(max(data) + 10)
fig = plt.figure()
ax = fig.add_subplot(111)

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%.2f'%height,
                ha='center', va='bottom')

xlocations = na.array(range(len(data))) + 0.5
width = 0.5

rects = ax.bar(xlocations, data, width=width)
ax.set_yticks(range(0, ticks))
ax.set_xticks(xlocations+ width/2)
ax.set_xticklabels(labels)
ax.set_xlim(0, xlocations[-1]+width*2)
ax.set_title("Loads")

def animate(i):
  global ticks
  with open("loads.json") as f:
    nd = json.load(f)
    
  #preserve order of the labels  
  new_data = [nd[l] for l in full_labels]
  if max(new_data) > ticks:
    ticks = int(max(new_data) + 10)
  ax.set_yticks(range(0, ticks))
  [rect.set_height(height) for rect, height in zip(rects, new_data)]

ani = animation.FuncAnimation(fig, animate, interval=4000)
plt.show()