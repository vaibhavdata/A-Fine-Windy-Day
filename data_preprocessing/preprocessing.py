from scipy import stats
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import PowerTransformer
from sklearn.preprocessing import RobustScaler
from datetime import datetime as dt
from category_encoders import TargetEncoder
from sklearn.utils import all_estimators
class Preprocessor:
    def __init__(self,file_object,logger_object):
        self.file_object =file_object
        self.logger_object =logger_object

    def is_null_present(self,data):
        self.logger_object.log(self.file_object,"Entered is Null Present in data set")
        self.null_present =False
        try:
            self.null_counts =data.isna().sum()
            for i in self.null_counts:
                if i >0:
                    self.null_present =True
                    break
                if (self.null_present):
                    df_with_null =pd.DataFrame()
                    df_with_null['column'] =data.columns
                    df_with_null['missingValueCount'] =np.array(data.isna().sum())
                    df_with_null.to_csv("preprocessing_data/null_values.csv")
                    self.logger_object.log(self.file_object,"Missing Value Found")
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception  Occured while performing is_null_present method %s' % e)
            self.logger_object.log(self.file_object,"Failed to find missing ")
            raise e
    def impute_missing_values(self,data):
        self.logger_object.log(self.file_object,"Enter Mission value impute")
        self.data = data
        try:
            self.data['wind_speed(m/s)'] =self.data['wind_speed(m/s)'].fillna(self.data['wind_speed(m/s)'].mean())
            self.data['atmospheric_temperature(°C)'] =self.data['atmospheric_temperature(°C)'].fillna(self.data['atmospheric_temperature(°C)'].mean())
            self.data['shaft_temperature(°C)']=self.data['shaft_temperature(°C)'].fillna(self.data['shaft_temperature(°C)'].mean())
            self.data['blades_angle(°)']=self.data['blades_angle(°)'].fillna(self.data['blades_angle(°)'].mean())
            self.data['gearbox_temperature(°C)']=self.data['gearbox_temperature(°C)'].fillna(self.data['gearbox_temperature(°C)'].mean())
            self.data['engine_temperature(°C)']=self.data['engine_temperature(°C)'].fillna(self.data['engine_temperature(°C)'].mean())
            self.data['motor_torque(N-m)']=self.data['motor_torque(N-m)'].fillna(self.data['motor_torque(N-m)'].mean())
            self.data['generator_temperature(°C)']=self.data['generator_temperature(°C)'].fillna(self.data['generator_temperature(°C)'].mean())
            self.data['atmospheric_pressure(Pascal)']=self.data['atmospheric_pressure(Pascal)'].fillna(self.data['atmospheric_pressure(Pascal)'].mean())
            self.data['area_temperature(°C)']=self.data['area_temperature(°C)'].fillna(self.data['area_temperature(°C)'].mean())
            self.data['windmill_body_temperature(°C)']=self.data['windmill_body_temperature(°C)'].fillna(self.data['windmill_body_temperature(°C)'].median())
            self.data['wind_direction(°)']=self.data['wind_direction(°)'].fillna(self.data['wind_direction(°)'].median())
            self.data['resistance(ohm)']=self.data['resistance(ohm)'].fillna(self.data['resistance(ohm)'].median())
            self.data['rotor_torque(N-m)']=self.data['rotor_torque(N-m)'].fillna(self.data['rotor_torque(N-m)'].mean())
            self.data['blade_length(m)']=self.data['blade_length(m)'].fillna(self.data['blade_length(m)'].median())
            self.data['blade_breadth(m)']=self.data['blade_breadth(m)'].fillna(self.data['blade_breadth(m)'].median())
            self.data['windmill_height(m)']=self.data['windmill_height(m)'].fillna(self.data['windmill_height(m)'].median())
            self.data['cloud_level']=self.data['cloud_level'].fillna(self.data['cloud_level'].value_counts().index[0])
            self.data['turbine_status']=self.data['turbine_status'].fillna(self.data['turbine_status'].value_counts().index[0])
            self.data['windmill_generated_power(kW/h)']=self.data['windmill_generated_power(kW/h)'].fillna(self.data['windmill_generated_power(kW/h)'].median())
            
            self.logger_object.log(self.file_object,"Missing value fill succesfully")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,"Error when fill missing value",str(e))
            self.logger_object.log(self.file_object,"error not fill missing value")
            raise Exception()

    def datetime_column(self,data):
        self.logger_object.log(self.file_object,"Convert datatime folder into int")
        self.data =data
        try:
            self.data['datetime'] =pd.to_datetime(self.data['datetime'])
            
            date_time_df = pd.DataFrame()

            date_time_df['month'] =self.data['datetime'].dt.month
            date_time_df['day'] =self.data['datetime'].dt.day
            date_time_df['year'] =self.data['datetime'].dt.year
            self.data = pd.concat([self.data,date_time_df], axis=1)
            self.logger_object.log(self.file_object,"datetime convert into succesfully")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,"Error convert datatime convert",str(e))
            self.logger_object.log(self.file_object,"error in datetime")
            raise Exception()

    def encoding_clould(self,data):
        self.data = data
        self.logger_object.log(self.file_object,'Entered to Data Row perform One-Hot Encoding on cloud Feature')

        try:
            #cloud_df = pd.get_dummies(self.data['cloud_level'],drop_first=True)
            #self.data = pd.concat([self.data,cloud_df],axis=1)
            cloud_level = self.data['cloud_level']
            if str(cloud_level) == 'Medium':
                self.data['Medium'] = 1
                self.data['Low'] = 0
                self.data['Extremely Low'] = 0
                
            elif str(cloud_level) == 'Low':
                self.data['Medium'] = 0
                self.data['Low'] = 0
                self.data['Extremely Low'] = 0
                
            elif str(cloud_level) == 'Extremely Low':
                self.data['Medium'] = 0
                self.data['Low'] = 0
                self.data['Extremely Low'] = 1
                
            
            else:
                self.data['Medium'] = 0
                self.data['Low'] = 0
                self.data['Extremely Low'] = 0
                

            self.logger_object.log(self.file_object,'One-Hot Encoding of Source Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing Data One-Hot Encoding over Source feature:: %s' % str(e))
            raise e
    
    
    def seprete_data_column(self,data,label_name):
        self.logger_object.log(self.file_object,"sterted columns")
        self.data =data
        try:
            self.X = self.data.drop([label_name],axis=1)
            self.Y =self.data[label_name]
            self.logger_object.log(self.file_object,"Sepreate label succesfully")
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,"Error in sepreted label",str(e))
            raise Exception()
    def scale_numberical_value(self,data):
        self.logger_object.log(self.file_object,"scale numberical value")
        self.data =data
        self.num_df=self.data[['wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)','blade_breadth(m)', 'windmill_height(m)']]
        try:

            self.scaler = RobustScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns,index=self.data.index)
            self.data.drop(columns=self.scaled_num_df.columns, inplace=True)
            self.data = pd.concat([self.scaled_num_df, self.data], axis=1)

            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()
    def remove_columns(self,data,columns):
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            self.logger_object.log(self.file_object,
                                   'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,
                                   'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise Exception()
        
            
            

    

    
    def drop_outliers(self,data):
        self.data =data
        cols = ['wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)']
        Q1 = self.data[cols].quantile(0.07)
        Q3 = self.data[cols].quantile(0.80)
        IQR = Q3 - Q1

        self.data = self.data[~((self.data[cols] < (Q1 - 1.5 * IQR)) |(self.data[cols] > (Q3 + 1.5 * IQR))).any(axis=1)]
        return self.data
    def remove_high_skew(self,data):
        self.data =data
        cols = ['wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)','resistance(ohm)','rotor_torque(N-m)', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)']
        Q1 = data[cols].quantile(0.22)
        Q3 = data[cols].quantile(0.)
        IQR = Q3 - Q1

        self.data = self.data[~((self.data[cols] < (Q1 - 1.5 * IQR)) |(self.data[cols] > (Q3 + 1.5 * IQR))).any(axis=1)]
        return self.data
    
    def remove_unwanted_cols(self,data,return_unwanted_data=False):
        self.logger_object.log(self.file_object,'Removing Unwanted Columns Started !!')
        self.df = data

        try:
            self.data = self.df.drop(['tracking_id', 'datetime','cloud_level','turbine_status'],axis=1)

            if return_unwanted_data == True:
                self.unwanted_data = self.df[['tracking_id', 'datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)']]

            self.logger_object.log(self.file_object,'Unwanted Columns Deleted Successfully !!')
            #self.logger_object.log(self.file_object,'Sample Feature with 1 Row')
            #self.logger_object.log(self.file_object,str(self.data.head(1)))

            if return_unwanted_data == True:
                return self.data,self.unwanted_data
            else:
                return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured while removing unwanted columns :: %s' %str(e))
            raise e
    def encoding_turbine(self,data):
        self.logger_object.log(self.file_object,'Entered to perform One-Hot Encoding on turbine_status  Feature')
        self.data = data

        try:
            self.encoder=TargetEncoder()
            self.data['turbine_status_encoding'] =self.encoder.fit_transform(self.data['turbine_status'],self.data['windmill_generated_power(kW/h)'])
            
            self.logger_object.log(self.file_object,'One-Hot Encoding of Browser Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing One-Hot Encoding over turibing  feature:: %s' %str(e))
            raise e 


    
