import pandas as pd
import matplotlib.pyplot as plt

X_TICKS = 7

dataset = pd.read_csv("datasets/netflix_marketvalue.csv", parse_dates=True)

x=[]
y=[]

for date in dataset['Date']:
    x.append(date)

for value in dataset['Open']:
    y.append(value)
    
plt.plot(x,y, color='darkorange')
plt.xticks(range(0, len(x), X_TICKS), x[::X_TICKS], rotation = 90)
plt.ylim(0, 600)
plt.title("marketvalue netflix", fontsize=10)
plt.xlabel('date')
plt.ylabel('value in dollar')
plt.tight_layout()
plt.show()
