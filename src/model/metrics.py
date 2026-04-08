import numpy as np

from sklearn.metrics import mean_absolute_error


def accuracy(y_true, y_pred):
    mae = mean_absolute_error(y_true,y_pred)
    mean_demand = np.mean(y_true)
    acc = (1- (mae / mean_demand)) * 100
    return acc 

def bias(y_true, y_pred):
    return np.mean(y_pred - y_true)

def report_evaluate_performance(model_name: str, y_true, y_pred):
    acc = np.round(accuracy(y_true, y_pred),2)
    b = np.round(bias(y_true, y_pred),2)
    mae = np.round(mean_absolute_error(y_true, y_pred),2)
    
    print(f'Accuracy of {model_name}: {acc}')
    print(f'Bias of {model_name}: {b}')
    print(f'Mean absolute error of {model_name}: {mae}')