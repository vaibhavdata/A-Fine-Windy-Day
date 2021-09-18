from sklearn.model_selection import train_test_split
from data_insert.data_loader import Data_Getter
from data_preprocessing.preprocessing import Preprocessor
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger
from data_preprocessing import clustering

#Creating the common Logging object


class TrainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            # Getting the data from the source
            self.log_writer.log(self.file_object, "start data getting")
            data_getter =Data_Getter(self.file_object,self.log_writer)
            data =data_getter.get_data()
            self.log_writer.log(self.file_object, "get data succesfullly")
            preprocess =Preprocessor(self.file_object,self.log_writer)
            is_null_present =preprocess.is_null_present(data)
            if is_null_present ==True:
                data = preprocess.impute_missing_values(data)
            #data =preprocess. remove_columns(data,['tracking_id'])
            
            self.log_writer.log(self.file_object, "import missing value succesfully")
            data = preprocess.datetime_column(data)
            self.log_writer.log(self.file_object,"inter in label encoding")
            data =preprocess.encoding_clould(data)
            data =preprocess.encoding_turbine(data)
            #self.log_writer.log(self.file_object,print(data.info()))
            self.log_writer.log(self.file_object,"Encoding succesfullly")
            data =preprocess.remove_columns(data,['datetime','cloud_level','tracking_id','turbine_status'])
            self.log_writer.log(self.file_object,"Remove date time column succesfully")
            #self.log_writer.log(self.file_object,print(data.skew()))
            #data =preprocess.remove_high_skewnessas(data)
            data =preprocess.drop_outliers(data)
            #data =preprocess.cbrt_transform(data)
            X,Y =preprocess.seprete_data_column(data,label_name='windmill_generated_power(kW/h)')
            
            x_train,x_test,y_train,y_test =train_test_split(X,Y,test_size =0.3)
            
            self.log_writer.log(self.file_object,"train test split")
            x_train =preprocess.scale_numberical_value(x_train)
            x_test =preprocess.scale_numberical_value(x_test)
            #self.log_writer.log(self.file_object,print(x_train.skew()))
            self.log_writer.log(self.file_object,"scale numbrical value succesfully")
            # model finder
            model_finder = tuner.ModelFinde(self.file_object,self.log_writer)
            best_model_name,best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)

            file_op = file_methods.File_Operation(self.file_object,self.log_writer)
            save_model = file_op.save_model(best_model,best_model_name)

            self.log_writer.log(self.file_object,'Successfull End of Training')
            self.file_object.close()
            

        except Exception:
            self.log_writer.log(self.file_object,"Error in training")
            raise Exception()
