# import libraries
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report, root_mean_squared_error, r2_score, mean_absolute_percentage_error
from imblearn.under_sampling import RandomUnderSampler
from sklearn.model_selection import GridSearchCV

class ModelDataGeneration:
    @staticmethod
    def under_sample_data(X, y):
        undersample = RandomUnderSampler(sampling_strategy = "majority", random_state=42) 
        x_under, y_under = undersample.fit_resample(X, y)
        return x_under, y_under
    @staticmethod
    def split_data(X, y, test_size = 0.3, random_state = 42) -> tuple:
        X_train, X_test, y_train, y_test =  train_test_split(X, y, test_size=test_size, random_state=random_state)
        X_val, X_test, y_val, y_test = train_test_split(X_test, y_test, test_size=0.5, random_state=random_state)
        return X_train, X_test, X_val, y_val,y_train, y_test

class ModelGeneration:
    
    def __init__(self, model, datasets):
        self.model = model
        self.X_train, self.X_test, self.X_val, self.y_val, self.y_train, self.y_test = datasets
        
    def fit_model(self, params = None):
        if params:
            self.model.set_params(**params)
        eval_set = [(self.X_train, self.y_train), (self.X_val, self.y_val)]
        if self.model.__class__.__name__ == "XGBClassifier":
            print("Using XGBoost")
            self.model.fit(self.X_train, self.y_train, eval_set = eval_set, eval_metric="auc")
        else:
            self.model.fit(self.X_train, self.y_train)
        return self.model 
    
    def fit_grid_search(self, param_grid, score_metric = "roc_auc"):
        grid_search = GridSearchCV(self.model, param_grid, scoring=score_metric, cv=5, n_jobs=-1)
        grid_search.fit(self.X_train, self.y_train)
        self.best_params_ = grid_search.best_params_
        self.best_model_ = grid_search.best_estimator_
        self.best_score_ = grid_search.best_score_
        return self.best_model_
    
    def predict_model(self):
        self.y_pred = self.model.predict(self.X_test)
    
    def test_model(self, type = "classification"):
        if type == "classification":
            self.predict_model()
            self.auc_score()
            return self.classification_report()
        elif type == "regression":
            self.predict_model()
            self.r2_score_result()
            self.root_mean_squared_error_score()
            self.mean_absolute_percentage_error_score()
            return None
    
    def root_mean_squared_error_score(self):
        rmse = root_mean_squared_error(self.y_test, self.y_pred)
        print("El RMSE es de " + str(rmse))
        return rmse
    
    def r2_score_result(self):
        r2 = r2_score(self.y_test, self.y_pred)
        print("El R2 score es de " + str(r2))
        return r2
    
    def mean_absolute_percentage_error_score(self):
        mape = mean_absolute_percentage_error(self.y_test, self.y_pred)
        print("El MAPE es de " + str(mape))
        return mape
    
    def auc_score(self):
        auc = roc_auc_score(self.y_test, self.y_pred)
        print("El AUC score es de " + str(auc))
        return roc_auc_score(self.y_test, self.y_pred)
    
    def classification_report(self):
        print(classification_report(self.y_test, self.y_pred))

    def feature_importance_xgb(self):
        feature_importance_scores = pd.DataFrame([self.model.get_booster().get_score(importance_type='gain')]).T.reset_index()
        feature_importance_scores.rename(columns={"index": "feature", 0: "importance"}, inplace=True)
        feature_importance_scores = feature_importance_scores.sort_values(by = "importance", ascending = False)
        feature_importance_scores["relative_importance"] = (feature_importance_scores['importance'] / feature_importance_scores['importance'].sum())
        return feature_importance_scores