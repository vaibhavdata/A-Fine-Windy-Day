
from application_logging.logger import App_Logger
from flask import Flask, request, render_template
from flask import Response
import os

from prediction_validation_insert import Predication_validation
from trainModel import TrainModel
from train_validation_insert import train_validation
from predictFromModel import  Prediction_Row
from predictFromModel import prediction
import json

import pandas as pd
app = Flask(__name__)
import shutil



@app.route("/", methods=["GET","POST"])

def home():
    return render_template('base.html')



@app.route("/predict", methods=['POST'])

def predictRouteClient():
    try:
        if request.json['filepath'] is not None:
            path = request.json['filepath']

            pred_val = Predication_validation(path) #object initialization

            pred_val.predication_validation() #calling the prediction_validation function

            predModelObj = prediction() #object initialization
            path,json_predictions = predModelObj.predictionFromModel() 
            return Response("Prediction File Created at !! " + str(path) + "and Few of the Predictions are" + str(json.loads(json_predictions)))
            
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)



@app.route("/train", methods=['POST','GET'])

def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
        #path ="train_Batch_File"

        train_valObj = train_validation(path) #object initialization

        train_valObj.train_validation()#calling the training_validation function


        trainModelObj = TrainModel() #object initialization
        trainModelObj.trainingModel() #training the model for the files in the table


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

@app.route("/predictRow",methods=['POST'])
def predictRowRoute():
    try:
        if request.method == 'POST':
            try:
                if request.form is not None:
                    if request.form['datetime'] is not None and request.form['windspeed'] is not None and request.form['atmospherictemperature'] is not None and request.form['shafttemperature'] is not None and request.form['bladesangle'] is not None and request.form['gearboxtemperature'] is not None and request.form['enginetemperature'] is not None and request.form['motortorque'] is not None and request.form['generatortemperature'] is not None and request.form['atmosphericpressure'] is not None and request.form['areatemperature'] is not None and request.form['windmillbodytemperature'] is not None and request.form['winddirection'] is not None and request.form['resistance'] is not None and request.form['rotortorque'] is not None and request.form['turbinestatus'] is not None and request.form['cloudlevel'] is not None and request.form['bladelength'] is not None and request.form['bladebreadth'] is not None and request.form['windmillheight'] is not None:
                        datetime  = request.form['datetime']
                        windspeed = request.form['windspeed']
                        atmospherictemperature = request.form['atmospherictemperature']
                        shafttemperature = request.form['shafttemperature']
                        bladesangle= request.form['bladesangle']
                        gearboxtemperature= request.form['gearboxtemperature']
                        enginetemperature= request.form['enginetemperature']
                        motortorque = request.form['motortorque']
                        generatortemperature = request.form['generatortemperature']
                        atmosphericpressure = request.form['atmosphericpressure']
                        areatemperature= request.form['areatemperature']
                        windmillbodytemperature = request.form['windmillbodytemperature']
                        winddirection = request.form['winddirection']
                        resistance = request.form['resistance']
                        rotortorque = request.form['rotortorque']
                        turbinestatus = request.form['turbinestatus']
                        cloudlevel = request.form['cloudlevel']
                        bladelength= request.form['bladelength']
                        bladebreadth = request.form['bladebreadth']
                        windmillheight = request.form['windmillheight']
                        data = pd.DataFrame([[datetime,windspeed,atmospherictemperature,shafttemperature,bladesangle,gearboxtemperature,enginetemperature,motortorque,generatortemperature,atmosphericpressure,areatemperature,windmillbodytemperature,winddirection,resistance,rotortorque,turbinestatus,cloudlevel,bladelength,bladebreadth,windmillheight]],
                                              columns=['datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)'])
                        
                        predict = Prediction_Row()
                        result = predict.predictRow(data)
                        return Response('Prediction is: ' + str(json.loads(result)))
            except Exception as e:
                raise e
    except Exception as e:
        print(e)
        return Response('Error Occured::%s' % str(e))
@app.route("/predictBatch",methods=['POST'])

def predictBatchRoute():
    try:
        if request.method == 'POST':
            #batchpath = request.form['batchpath']
            print(request.files)
            cwd = os.getcwd()
            try:
                if 'file' in request.files:
                    batch_file = request.files['file']
                    #path = os.path.join(os.getcwd(),)
                    if os.path.exists('predication_Batch_File'):
                        file = os.listdir('predication_Batch_File')
                        if not len(file) == 0:
                            os.remove('predication_Batch_File/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Database'):
                        file = os.listdir('Prediction_Database')
                        if not len(file) == 0:
                            os.remove('Prediction_Database/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Logs'):
                        file = os.listdir('Prediction_Logs')
                        if not len(file) == 0:
                            for f in file:
                                os.remove('Prediction_Logs/' + f)
                    else:
                        pass

                    if os.path.exists('Prediction_Output_File'):
                        file = os.listdir('Prediction_Output_File')
                        if not len(file) == 0:
                            os.remove('Prediction_Output_File/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Raw_files_validated/Bad_Raw'):
                        file = os.listdir('Prediction_Raw_files_validated/Bad_Raw')
                        if not len(file) == 0:
                            os.remove('Prediction_Raw_files_validated/Bad_Raw/' + file[0])
                    else:
                        pass

                    

                    if os.path.exists('PredictionArchiveBadData'):
                        shutil.rmtree('PredictionArchiveBadData')
                    else:
                        pass

                   

                    if os.path.exists('PredictionFileFromDB'):
                        file = os.listdir('PredictionFileFromDB')
                        if not len(file) == 0:
                            os.remove('PredictionFileFromDB/' + file[0])
                    else:
                        pass

                    

                    batch_file.save('predication_Batch_File/' + batch_file.filename)
                    print('Uploaded Successfully !!')
            except Exception as e:
                print(e)

            except OSError as o:
                print(str(o))

            MainFilePath = 'predication_Batch_File' + '/' + batch_file.filename
            

            prediction_val = Predication_validation('predication_Batch_File')
            prediction_val.predication_validation()

            pred = prediction(MainFilePath)
            path = pred.predictionFromModel()
            
            return Response("Prediction File Created at !! " + str(path))
            return Response('Done')
        else:
            print('None Request Matched')
    except ValueError:
        return Response('Error Occured! %s' % str(ValueError))
    except KeyError:
        return Response('Error Occured! %s' % str(KeyError))
    except Exception as e:
        return Response('Error Occured! %s' % str(e))


if __name__ == "__main__":
    app.run(debug=True)
