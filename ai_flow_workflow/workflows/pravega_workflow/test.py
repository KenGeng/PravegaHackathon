import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# IMG_COLUMNS = ['label', 'img']

train_data = pd.read_csv('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test.csv', header=0)
# print(train_data)
y_train = train_data.pop('label')
# print(y_train)
clf = LogisticRegression(C=50. / 5000, penalty='l1', solver='saga', tol=0.1)
# train_data['img'] = train_data['img'].apply(lambda x : [float(y) for y in x.split(' ')] )
# print(train_data['img'].values[0])
# train_data = train_data['img'].apply(np.fromstring)
dd = train_data['img'].values
# dd = [ [float(y)] for i in dd for y in i.split(' ') ]
print(dd)

t = []
t = [[float(j) for j in i.split(' ') ] for i in dd]

# t = train_data['img'].apply(lambda x : np.ndarray([float(y) for y in x.split(' ')]) ).values
# for i in dd:
#     t.append([float(j) for j in i.split(' ') ])
print(t)
clf.fit(t, y_train.values)
print(clf)


# test_data = pd.read_csv('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test.csv', header=0)
# # Prepare dataset
# y_test = test_data.pop(0)
#
#
# print(clf.predict(test_data))