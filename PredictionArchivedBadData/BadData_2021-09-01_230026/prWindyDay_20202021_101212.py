import pandas as pd

class Getter_Data:
    def __init__(self,file_object,logger_object):
        self.file ="Prediction_FileFromDB/InputFile.csv"
        self.file_object =file_object
        self.logger_object =logger_object
    def get_data(self):
        self.logger_object.log(self.file_object,"Get data from Getter_data metod")
        try:
            self.data = pd.read_csv(self.file)
            self.logger_object.log(self.file_object,"Get data succesfully !!")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,"Error in get data methods  !!!",str(e))
            self.logger_object.log(self.file_object,"Not get from test file se")
            raise Exception()
            
