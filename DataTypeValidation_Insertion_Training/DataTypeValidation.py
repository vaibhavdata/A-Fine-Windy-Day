
import shutil
import pymongo
import os
import pandas as pd
from application_logging.logger import App_Logger

class DB_Operations:
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        self.path = 'Training_Database'
        self.good_file_path = 'Training_Raw_files_validated/Good_Raw'
        self.bad_file_path = 'Training_Raw_files_validated/Bad_Raw'
        self.FileFromDB = 'TrainingFileFrom_DB'
        self.logger = App_Logger()

    def create_db_connection(self,database_name):
        file = open('Training_Logs/General_Log.txt', 'a+')
        self.logger.log(file,'Entered create_db_connection() method of DB_Operation class of training_db_operations package')
        file.close()

        try:
            self.db_object = self.client[str(database_name)]
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Database < %s > Created successfully!!" % str(database_name))
            self.logger.log(file, "Database < %s > Connected successfully!!" % str(database_name))
            file.close()

            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file,'Successfully Executed create_db_connection() method of DB_Operation class of db_operation package')
            file.close()
            return self.db_object
        except Exception as ex:
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file, 'Error while settingup connection with database.Error :: %s' % ex)
            file.close()


    def create_collection(self,db_object):
        file = open('Training_Logs/General_Log.txt', 'a+')
        self.logger.log(file,'Entered create_collection() method of DB_Operation class of training_db_operations package')
        file.close()

        try:
            collection_list = db_object.collection_names()
            collection_name = 'GoodRawData'
            file = open("Training_Logs/CreateCollectionLog.txt", 'a+')
            if collection_name in collection_list:
                collection_object = db_object[collection_name]
                collection_object.remove({})
                self.logger.log(file,'GoodRawData Collection already Exist and deleted documents Successfully!!')
            else:
                collection_object = db_object.create_collection(collection_name)
                self.logger.log(file,'GoodRawData Collection created Successfully !!')

            file.close()
            file = open('Training_Logs/General_Log.txt', 'a+')
            self.logger.log(file,'Successfully Executed create_collection() method of DB_Operation class of db_operation package')
            file.close()
            return collection_object
        except Exception as ex:
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file, 'Error while creating collection in database.Error :: %s' % ex)
            file.close()

    def insertion_GoodRawData_into_collection(self,collection_object):
        file = open('Training_Logs/General_Log.txt', 'a+')
        self.logger.log(file, 'Entered insertionGoodData_into_collection() method of DB_Operation class of training_db_operations package')
        file.close()

        try:
            only_files = [file for file in os.listdir(self.good_file_path)]
            file = open("Training_Logs/DataBaseSelectionLog.txt", 'a+')
            for f in only_files:
                
                data = pd.read_csv(os.path.join(self.good_file_path,f))
                document = [{'tracking_id':id,'datetime':date,'wind_speed(m/s)':wind,'atmospheric_temperature(°C)':atmos,'shaft_temperature(°C)':shaft,'blades_angle(°)':blades,'gearbox_temperature(°C)':gerabox,'engine_temperature(°C)':engine,'motor_torque(N-m)':motor,'generator_temperature(°C)':generator,'atmospheric_pressure(Pascal)':atmosperic,'area_temperature(°C)':area,'windmill_body_temperature(°C)':windmill,'wind_direction(°)':winds,'resistance(ohm)':resi,'rotor_torque(N-m)':rotor,'turbine_status':turbine,'cloud_level':cloud,'blade_length(m)':blade,'blade_breadth(m)':blade_b,'windmill_height(m)':windmill,'windmill_generated_power(kW/h)':windmill_gen} for id,date,wind,atmos,shaft,blades,gerabox,engine,motor,generator,atmosperic,area,windmill,winds,resi,rotor,turbine,cloud,blade,blade_b,windmill,windmill_gen in zip(data['tracking_id'],data['datetime'],data['wind_speed(m/s)'],data['atmospheric_temperature(°C)'],data['shaft_temperature(°C)'],data['blades_angle(°)'],data['gearbox_temperature(°C)'],data['engine_temperature(°C)'],data['motor_torque(N-m)'],data['generator_temperature(°C)'],data['atmospheric_pressure(Pascal)'],data['area_temperature(°C)'],data['windmill_body_temperature(°C)'],data['wind_direction(°)'],data['resistance(ohm)'],data['rotor_torque(N-m)'],data['turbine_status'],data['cloud_level'],data['blade_length(m)'],data['blade_breadth(m)'],data['windmill_height(m)'],data['windmill_generated_power(kW/h)'])]
                collection_object.insert_many(document)
                
                self.logger.log(file,'Data File :: %s Inserted Successfully in Collection'.format(f))

            file.close()
            file = open('Training_Logs/General_Log.txt', 'a+')
            self.logger.log(file,'Successfully Executed insertion_GoodRawData_into_collection() method of DB_Operation class of db_operation package')
            file.close()
        except Exception as ex:
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file, 'Error while inserting data file into collection.Error :: %s' % ex)
            file.close()
    

    def selectDataFromCollection_into_csv(self, collection_object):
        file = open('Training_Logs/General_Log.txt', 'a+')
        self.logger.log(file,'Entered selectDataFromCollection_into_csv() method of DB_Operation class of training_db_operations package')
        file.close()

        try:
            data = list()
            for i in collection_object.find():
                data.append({'tracking_id':i['tracking_id'],'datetime':i['datetime'],'wind_speed(m/s)':i['wind_speed(m/s)'],'atmospheric_temperature(°C)':i['atmospheric_temperature(°C)'],'shaft_temperature(°C)':i['shaft_temperature(°C)'],'blades_angle(°)':i['blades_angle(°)'],'gearbox_temperature(°C)':i['gearbox_temperature(°C)'],'engine_temperature(°C)':i['engine_temperature(°C)'],'motor_torque(N-m)':i['motor_torque(N-m)'],'generator_temperature(°C)':i['generator_temperature(°C)'],'atmospheric_pressure(Pascal)':i['atmospheric_pressure(Pascal)'],'area_temperature(°C)':i['area_temperature(°C)'],'windmill_body_temperature(°C)':i['windmill_body_temperature(°C)'],'wind_direction(°)':i['wind_direction(°)'],'resistance(ohm)':i['resistance(ohm)'],'rotor_torque(N-m)':i['rotor_torque(N-m)'],'turbine_status':i['turbine_status'],'cloud_level':i['cloud_level'],'blade_length(m)':i['blade_length(m)'],'blade_breadth(m)':i['blade_breadth(m)'],'windmill_height(m)':i['windmill_height(m)'],'windmill_generated_power(kW/h)':i['windmill_generated_power(kW/h)']})
    
    

            if not os.path.isdir(self.FileFromDB):
                os.makedirs(self.FileFromDB)

            dataframe = pd.DataFrame(data,columns=['tracking_id', 'datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)','windmill_generated_power(kW/h)'])
            dataframe.to_csv(os.path.join(self.FileFromDB,'InputFile.csv'),index=False)

            file = open("Training_Logs/DataBase_Into_CSVLog.txt", 'a+')
            self.logger.log(file,'CSV File Exported Successfully !!!')
            file.close()

            file = open('Training_Logs/General_Log.txt', 'a+')
            self.logger.log(file,'Successfully Executed selectDataFromCollection_into_csv() method of DB_Operation class of db_operation package')
            file.close()
        except Exception as ex:
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file, 'Error while selecting data file and store it as csv.Error :: %s' % ex)
            file.close()









