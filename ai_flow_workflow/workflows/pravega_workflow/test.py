import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# IMG_COLUMNS = ['label', 'img']

train_data = pd.read_csv('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_train.csv', header=None)
print(train_data[0])
y_train = train_data.pop(0)
print(y_train)
clf = LogisticRegression(C=50. / 5000, penalty='l1', solver='saga', tol=0.1)
# train_data = train_data['img'].apply(np.fromstring)
clf.fit(train_data.values, y_train.values)
print(clf)


test_data = pd.read_csv('/Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test.csv', header=None)
# Prepare dataset
y_test = test_data.pop(0)


print(clf.predict(test_data))