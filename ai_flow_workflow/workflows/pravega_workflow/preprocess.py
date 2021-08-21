import numpy as np
import pandas as pd
# data = np.load('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_predict.npz')

def npz2csv(data, key:str):
    tmp = []
    imgs = data.get(key)
    print(len(imgs))
    for i in range(0, len(imgs)):
        tmp.append([data.get(key.replace('x', 'y'))[i], imgs[i]])
    df = pd.DataFrame(tmp)
    df.to_csv(key + ".csv", index=False, header=None)

# npz2csv(data, 'x_train')
# npz2csv(data, 'x_test')
df = pd.read_csv('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test.csv', header=None)
df['ColumnA'] = df[df.columns[1:]].apply(
    lambda x: ' '.join(x.dropna().astype(str)),
    axis=1
)
tt = pd.DataFrame()
tt['label'] = df[0]
tt['img'] = df['ColumnA']
print(tt)
tt.to_csv("mnist_test2.csv", index=False)