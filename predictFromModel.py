import pandas as pd
from pandas.core.accessor import delegate_names
from data_insert.data_loader_predication import Data_Getter
from data_preprocessing.pred_preproessing import Preprocessor
from data_preprocessing.pred_preproessing import PreProcessorRow
from file_operations import file_methods
from application_logging.logger import App_Logger
from predication_raw_validation.raw_validation import Raw_Data_validation
class prediction:
    def __init__(self):
        self.file_object = open("Predication_Logs/prediction_model.txt", "a+")
        self.log_writer = App_Logger()
        #self.pred_data_val = Raw_Data_validation(path)
    def predictionFromModel(self):
        try:
            self.log_writer.log(self.file_object, "start data getting")
            data_getter =Data_Getter(self.file_object,self.log_writer)
            data = data_getter.get_data()
            
            self.log_writer.log(self.file_object, "get data succesfullly")
            preprocess =Preprocessor(self.file_object,self.log_writer)
            is_null_present =preprocess.is_null_present(data)
            if is_null_present ==True:
                data = preprocess.impute_missing_values(data)
            
            self.log_writer.log(self.file_object, "import missing value succesfully")
            data = preprocess.datetime_column(data)
            self.log_writer.log(self.file_object,"inter in label encoding")
            data =preprocess.encoding_clould(data)
            data =preprocess.encoding_turbine(data)
            
            self.log_writer.log(self.file_object,"Remove date time column succesfully")
            data,unwanted_data= preprocess.remove_unwanted_cols(data,return_unwanted_data=True)
            
            
            self.log_writer.log(self.file_object,"remove outlier succesfully")
            file_loader = file_methods.File_Operation(self.file_object,self.log_writer)
            #save_model = file_op.save_model(best_model,best_model_name)
            
            model_name = file_loader.find_correct_model_file()
            model = file_loader.load_model(model_name)
            self.log_writer.log(self.file_object,"load model succesfully")
            result = list(model.predict(data))
            self.log_writer.log(self.file_object,"model load succesfully")
            
            
            data = list(zip(unwanted_data['tracking_id'],unwanted_data['datetime'],unwanted_data['wind_speed(m/s)'],unwanted_data['atmospheric_temperature(°C)'], unwanted_data['shaft_temperature(°C)'],unwanted_data['blades_angle(°)'],unwanted_data['gearbox_temperature(°C)'], unwanted_data['engine_temperature(°C)'],unwanted_data['motor_torque(N-m)'], unwanted_data['generator_temperature(°C)'],unwanted_data['atmospheric_pressure(Pascal)'], unwanted_data['area_temperature(°C)'],unwanted_data['windmill_body_temperature(°C)'],unwanted_data['wind_direction(°)'],unwanted_data['resistance(ohm)'],unwanted_data['rotor_torque(N-m)'],unwanted_data['turbine_status'],unwanted_data['cloud_level'], unwanted_data['blade_length(m)'],unwanted_data['blade_breadth(m)'], unwanted_data['windmill_height(m)'],result))
            result = pd.DataFrame(data,columns=['tracking_id', 'datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)','Predication'])
            path = "Prediction_Output_File/Prediction.csv"
            result.to_csv(path,header=True,mode='a+')
            self.log_writer.log(self.file_object,'Successfull End of Prediction')
            self.file_object.close()
        except Exception as e:
            self.log_writer.log(self.file_object,'Error Occured while doing the Prediction !! Error :: %s' %str(e))
            self.file_object.close()
            raise e
        return path,result.head().to_json(orient="records")

class Prediction_Row:
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open('Predication_Logs/Predictionlog.txt', 'a+')
        
    def predictRow(self,data):
        self.log_writer.log(self.file_object, 'Start of DataRow Prediction')
        self.data = data
        try:
            preprocess = PreProcessorRow(self.data,self.file_object,self.log_writer)
            self.data = preprocess.datetime_column(self.data)
            self.log_writer.log(self.file_object,"inter in label encoding")
            self.data =preprocess.encoding_clould(self.data)
            self.data =preprocess.encoding_turbine(self.data)
            #self.log_writer.log(self.file_object,print(self.data.info()))
            self.data= preprocess.remove_unwanted_cols(self.data)
            #self.data['wind_speed(m/s)'] =pd.to_numeric(self.data['wind_speed(m/s)'])
            #self.data['atmospheric_temperature(°C)'] =pd.to_numeric(self.data['atmospheric_temperature(°C)'])
            #self.data['blades_angle(°)'] =pd.to_numeric(self.data['blades_angle(°)'])
            self.data[['wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)','blade_length(m)','blade_breadth(m)', 'windmill_height(m)']] = self.data[['wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)']].apply(pd.to_numeric)
            #self.log_writer.log(self.file_object,print(self.data.info()))
            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            model_name = file_loader.find_correct_model_file()
            model = file_loader.load_model(model_name)
            result = model.predict(self.data)
            self.log_writer.log(self.file_object, 'Successfull End of DataRow Prediction')
            self.file_object.close()
        except Exception as e:
            self.log_writer.log(self.file_object, 'Error Occured while doing the DataRaw Prediction !! Error :: %s' % str(e))
            self.file_object.close()
            raise e
        return str(result[0])