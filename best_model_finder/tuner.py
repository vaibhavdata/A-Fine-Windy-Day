from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.ensemble import GradientBoostingRegressor
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
import sklearn.base as skb
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import RandomizedSearchCV
import numpy as np
from sklearn.metrics import mean_absolute_error,mean_squared_error
from xgboost.core import Booster

class ModelFinde:
    def __init__(self,file_object,logger_object):
        self.file_object =file_object
        self.logger_object =logger_object
        self.xgb =XGBRegressor(objective='reg:squarederror',)
        self.rf =RandomForestRegressor()
        self.gd =GradientBoostingRegressor()

    def get_best_params_for_gradient_Booster(self,x_train,y_train):
        self.logger_object.log(self.file_object,'Entered the get_best_params_for_RandomForest Method')
        try:
            
            self.gd =GradientBoostingRegressor(criterion='mse',random_state=330,max_depth=8,
                                     n_estimators=500,min_samples_split=2,min_samples_leaf=2)
            self.gd.fit(x_train,y_train)
            self.logger_object.log(self.file_object,'Gradient Boosting Regression Best params')
            return self.gd
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception Occured in get_best_params_for_gradentBoosting  ::%s' % str(e))
            #elf.logger_object.log(self.file_object,'RandomForest Parameter Tuning Failed.Exited !!')
            raise e





    def get_best_params_for_random_forest(self,x_train,y_train):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_random_forest method of the Model_Finder class')
        try:
            self.param_distributions = {
                "n_estimators" : [500,800,1000],
                "max_features" : ['sqrt'],
                "max_depth" : [int(x) for x in np.linspace(start=7,stop=12,num=3)],
                "min_samples_split" : [2,3],
                "min_samples_leaf" : [2,4]
            }

            self.grid = RandomizedSearchCV(estimator=self.rf,param_distributions=self.param_distributions,cv=2,n_jobs=1,verbose=30)
            self.grid.fit(x_train,y_train)

            #self.criterion = self.grid.best_params_['criterion']
            self.n_estimators = self.grid.best_params_['n_estimators']
            #self.max_features = self.grid.best_params_['max_features']
            self.booster = self.grid.best_params_['booster']
            self.min_samples_split = self.grid.best_params_['min_samples_split']
            self.max_depth = self.grid.best_params_['max_depth']
            self.min_samples_leaf = self.grid.best_params_['min_samples_leaf']

            self.rf = RandomForestRegressor(n_estimators=self.n_estimators,max_depth=self.max_depth,min_samples_leaf=self.min_samples_leaf,min_samples_split=self.min_samples_split)
            self.rf.fit(x_train,y_train)
            self.logger_object.log(self.file_object,"Random Forest Predict")
            return self.rf
            
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_random_forest method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Random Forest Parameter tuning  failed. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise (e)
                                   
    def get_best_params_for_xgboost(self,x_train,y_train):
        self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            # creating a new model with the best parameters
            self.xgb = XGBRegressor(n_estimators=500,max_depth=5,booster='gbtree',n_jobs=-1,learning_rate=0.1,reg_lambda=0.01,reg_alpha=0.3)
            # training the mew model
            self.xgb.fit(x_train, y_train)
            self.logger_object.log(self.file_object,"fridict xgboost score ")
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()
    


    def get_best_model(self,x_train,y_train,x_test,y_test):
        
        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # create best model for XGBoost
        try:
            self.xgboost= self.get_best_params_for_xgboost(x_train,y_train)
            self.prediction_xgboost = self.xgboost.predict(x_test) # Predictions using the XGBoost Model

            if len(y_test.unique()) == 1:
                self.xgb_mean_score = mean_squared_error(y_test, self.prediction_xgboost)
                self.xgb_mean_squred_error = mean_absolute_error(y_test,self.prediction_xgboost)
                self.xgb_sqret_score = np.sqrt(mean_squared_error(y_test,self.prediction_xgboost))
                self.logger_object.log(self.file_object, 'mean squred error for XGBoost:' + str(self.xgb_mean_score )+ '\t' + "Mean Absolute Error " + str(self.xgb_mean_squred_error) + '\t' + "Mean sqret score" + str(self.xgb_sqret_score)) # Log AUC
            else:
                self.xgb_r2_score = r2_score(y_test, self.prediction_xgboost)
                self.xgb_mean_squred_error = mean_absolute_error(y_test,self.prediction_xgboost)
                self.xgb_sqret_score = np.sqrt(mean_squared_error(y_test,self.prediction_xgboost))
                self.logger_object.log(self.file_object, 'r2_score   for XGBoost:' + str(self.xgb_r2_score)+ '\t' + "Mean Absolute Error " + str(self.xgb_mean_squred_error) + '\t' + "Mean sqret score" + str(self.xgb_sqret_score))

            # create best model for Random Forest
            #self.random_forest=self.get_best_params_for_random_forest(x_train,y_train)
            #self.prediction_random_forest=self.random_forest.predict(x_test) # prediction using the Random Forest Algorithm

            #if len(y_test.unique()) == 1:#if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                #self.random_forest_score = mean_squared_error(y_test,self.prediction_random_forest)
                #self.random_forest_mae_error = mean_absolute_error(y_test,self.prediction_random_forest)
                #self.random_forest_sqrt_score =np.sqrt(mean_squared_error(y_test,self.prediction_random_forest))
                #self.logger_object.log(self.file_object, 'mse score for RF:' + str(self.random_forest_score)+'\t' + "Mean Absolute Error " + str(self.random_forest_mae_error) + '\t' + "Mean sqret score" + str(self.random_forest_sqrt_score))
            #else:
                #self.random_forest__r2_score = r2_score(y_test, self.prediction_random_forest)
                #self.random_forest_mae_error = mean_absolute_error(y_test,self.prediction_random_forest)
                #self.random_forest_sqrt_score =np.sqrt(mean_squared_error(y_test,self.prediction_random_forest))
                #self.logger_object.log(self.file_object, 'mean_score error:' + str(self.random_forest__r2_score)+'\t' + "Mean Absolute Error " + str(self.random_forest_mae_error) + '\t' + "Mean sqret score" + str(self.random_forest_sqrt_score))

            #create best model for gredient bossting
            self.gradient_boosting=self.get_best_params_for_gradient_Booster(x_train,y_train)
            self.prediction_gradient_boosting=self.gradient_boosting.predict(x_test) # prediction using the Random Forest Algorithm

            if len(y_test.unique()) == 1:#if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.gradient_boosting_score = mean_squared_error(y_test,self.prediction_gradient_boosting)
                self.gradient_boosting_mae_error = mean_absolute_error(y_test,self.prediction_gradient_boosting)
                self.gradient_boosting_sqrt_score =np.sqrt(mean_squared_error(y_test,self.prediction_gradient_boosting))
                self.logger_object.log(self.file_object, 'mse score for RF:' + str(self.gradient_boosting_score)+'\t' + "Mean Absolute Error " + str(self.gradient_boosting_mae_error) + '\t' + "Mean sqret score" + str(self.gradient_boosting_sqrt_score))
            else:
                self.gradient_boosting__r2_score = r2_score(y_test, self.prediction_gradient_boosting)
                self.gradient_boosting_mae_error = mean_absolute_error(y_test,self.prediction_gradient_boosting)
                self.gradient_boosting_sqrt_score =np.sqrt(mean_squared_error(y_test,self.prediction_gradient_boosting))
                self.logger_object.log(self.file_object, 'r2 score:' + str(self.gradient_boosting__r2_score)+'\t' + "Mean Absolute Error " + str(self.gradient_boosting_mae_error) + '\t' + "Mean sqret score" + str(self.gradient_boosting_sqrt_score))

            #comparing the two models
            if(self.gradient_boosting__r2_score <  self.xgb_r2_score):
                return 'XGBoost',self.xgb
            else:
                return 'GridentDescent',self.gd

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()
                                  