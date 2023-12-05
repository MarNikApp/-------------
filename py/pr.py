import pandas as pd 
import matplotlib.pyplot as plt

df = pd.read_excel("py\\test.xlsx")

plt.plot(df['date'], df['Уровень 4'], data=[df['new_rto_grs'], df['old_rto_grs']])
plt.show()