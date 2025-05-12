import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_curve
from sklearn.metrics import confusion_matrix
import seaborn as sns
import joblib



# svcs = ['adservice','cartservice','checkoutservice','currencyservice','emailservice','frontend','paymentservice','productcatalogservice','recommendationservice','shippingservice']
columns=[]
def data_loader(path):
    df = pd.read_csv(path)
    cols = [col for col in df.columns if col.endswith('&qps') or col.endswith('&count') or col == 'p90' or col.endswith('&receive') or col.endswith('&trainsmit')  or col == 'p90' or col.endswith('suc') or col.endswith('cpu') or col.endswith('mem') or col=='vCPU' or col=='memory']
    df = df[cols].dropna(axis=0, how='any')
    svcsQPS = [col.replace('&qps', '') for col in df.columns if col.endswith('&qps')]
    svcsQPS.sort()
    svcsCount=[col.replace('&count', '') for col in df.columns if col.endswith('&count')]
    svcsCount.sort()
    print(svcsQPS)
    print(svcsCount)
    # build dataset
    datas_x = []
    datas_y = []
    for _, row in df.iterrows():
        x = []
        for i in range(len(svcsQPS)):
            svc = svcsQPS[i]
            x.extend([row[svc + '&qps']])
        for i in range(len(svcsCount)):
            svc=svcsCount[i]
            x.extend([row[svc+'&count']])
            '''
        for i in range(len(svcsCount)):
            svc=svcsCount[i]
            x.extend([row[svc+'&cpu']])
        for i in range(len(svcsCount)):
            svc=svcsCount[i]
            x.extend([row[svc+'&mem']])
            '''
        datas_y.append(row['p90'])
        datas_x.append(x)

    datas_x = np.array(datas_x)
    datas_y = np.array(datas_y)
    print (datas_x)
    print(datas_y)
    return datas_x, datas_y

def try_different_method(model,x_train,y_train,x_test,y_test,name,datas_x,datas_y):
    model.fit(x_train,y_train)
    score = model.score(x_test, y_test)
    result = model.predict(x_test)
    joblib.dump(model, 'boutique/temp/'+name+'.model') 
    print('r2', r2_score(y_test, result))
    print('MAE', mean_absolute_error(y_test, result))
    print('MSE', mean_squared_error(y_test, result))
    print (cross_val_score(model,datas_x,datas_y,cv=10).mean())



###########3.具体方法选择##########
####3.1决策树回归####
from sklearn import tree
model_DecisionTreeRegressor = tree.DecisionTreeRegressor()
  
####3.2线性回归####
from sklearn import linear_model
model_LinearRegression = linear_model.LinearRegression()
####3.3SVM回归####
from sklearn import svm
model_SVR = svm.SVR()
####3.4KNN回归####
from sklearn import neighbors
model_KNeighborsRegressor = neighbors.KNeighborsRegressor()
####3.5随机森林回归####
from sklearn import ensemble
model_RandomForestRegressor = ensemble.RandomForestRegressor(n_estimators=20)#这里使用20个决策树
####3.6Adaboost回归####
from sklearn import ensemble
model_AdaBoostRegressor = ensemble.AdaBoostRegressor(n_estimators=50)#这里使用50个决策树
####3.7GBRT回归####
from sklearn import ensemble
model_GradientBoostingRegressor = ensemble.GradientBoostingRegressor(n_estimators=100)#这里使用100个决策树
####3.8Bagging回归####
from sklearn.ensemble import BaggingRegressor
model_BaggingRegressor = BaggingRegressor()
####3.9ExtraTree极端随机树回归####
from sklearn.tree import ExtraTreeRegressor
model_ExtraTreeRegressor = ExtraTreeRegressor()

if __name__ == '__main__':

    model = LinearRegression()
    datas_x, datas_y = data_loader('boutique.csv')
    X_train, X_test, y_train, y_test = train_test_split(
        datas_x, datas_y, test_size=0.2, random_state=10)
    try_different_method(model_DecisionTreeRegressor,X_train,y_train,X_test,y_test,'model_DecisionTreeRegressor',datas_x,datas_y)
    try_different_method(model_LinearRegression,X_train,y_train,X_test,y_test,'model_LinearRegression',datas_x,datas_y)
    try_different_method(model_SVR,X_train,y_train,X_test,y_test,'model_SVR',datas_x,datas_y)
    try_different_method(model_KNeighborsRegressor,X_train,y_train,X_test,y_test,'model_KNeighborsRegressor',datas_x,datas_y)
    try_different_method(model_RandomForestRegressor,X_train,y_train,X_test,y_test,'model_RandomForestRegressor',datas_x,datas_y)
    try_different_method(model_AdaBoostRegressor,X_train,y_train,X_test,y_test,'model_AdaBoostRegressor',datas_x,datas_y)
    try_different_method(model_GradientBoostingRegressor,X_train,y_train,X_test,y_test,'model_GradientBoostingRegressor',datas_x,datas_y)
    try_different_method(model_BaggingRegressor,X_train,y_train,X_test,y_test,'model_BaggingRegressor',datas_x,datas_y)
    try_different_method(model_ExtraTreeRegressor,X_train,y_train,X_test,y_test,'model_ExtraTreeRegressor',datas_x,datas_y)

