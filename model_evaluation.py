import model
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt

eval_set = [(model.X_train, model.y_train), (model.X_test, model.y_test)]

model.model.fit(
    model.X_train, model.y_train,
    eval_set=eval_set,
    verbose=False
)

results = model.model.evals_result()
train_rmse = results["validation_0"]["rmse"]
val_rmse = results["validation_1"]["rmse"]


plt.plot(train_rmse, label="Train RMSE")
plt.plot(val_rmse, label="Validation RMSE")
plt.legend()
plt.show()
