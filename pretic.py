@app.route("/predictRow",methods=['POST'])

def predict():
    try:
        if request.form is not None:
            if request.form['datetime'] is not None and request.form['wind_speed(m/s)'] is not None and request.form['atmospheric_temperature(°C)'] is not None and request.form['shaft_temperature(°C)'] is not None and request.form['blades_angle(°)'] is not None and request.form['gearbox_temperature(°C)'] is not None and request.form['engine_temperature(°C)'] is not None and request.form['motor_torque(N-m)'] is not None and request.form['windmill_body_temperature(°C)'] is not None and request.form['generator_temperature(°C)'] is not None and request.form['atmospheric_pressure(Pascal)'] is not None and request.form['area_temperature(°C)'] is not None and request.form['windmill_body_temperature(°C)'] is not None and request.form['motor_torque(N-m)'] is not None and request.form['motor_torque(N-m)'] is not None and request.form['motor_torque(N-m)'] is not None and request.form['wind_direction(°)'] is not None and request.form['resistance(ohm)'] is not None and request.form['rotor_torque(N-m)'] is not None and request.form['turbine_status'] is not None and request.form['cloud_level'] is not None and request.form['blade_length(m)'] is not None and request.form['blade_breadth(m)'] is not None and request.form['windmill_height(m)'] is not None:
                
                datetime  = request.form['datetime']
                wind_speedd = request.form['wind_speed(m/s)']
                atmos_t = request.form['atmospheric_temperature(°C)']
                shaft = request.form['shaft_temperature(°C)']
                blades= request.form['blades_angle(°)']
                gearbox = request.form['gearbox_temperature(°C)']
                engine_tem= request.form['engine_temperature(°C)']
                motor = request.form['motor_torque(N-m)']
                generat = request.form['generator_temperature(°C)']
                atmos_press = request.form['atmospheric_pressure(Pascal)']
                area_tem= request.form['area_temperature(°C)']
                windmill_bod = request.form['windmill_body_temperature(°C)']
                wind_dir = request.form['wind_direction(°)']
                resistance = request.form['resistance(ohm)']
                rotor = request.form['rotor_torque(N-m)']
                turbine = request.form['turbine_status']
                cloud_le = request.form['cloud_level']
                blade_len= request.form['blade_length(m)']
                blades_bre = request.form['blade_breadth(m)']
                windmill = request.form['windmill_height(m)']
                print(windmill)

                data = pd.DataFrame(
                    [[datetime,wind_speedd,atmos_t,shaft,blades,gearbox,engine_tem,motor,generat,atmos_press,area_tem,windmill_bod,wind_dir,resistance,rotor,turbine,cloud_le,blade_len,blades_bre,windmill]],
                    columns=['datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)'])

                predict = Prediction_Row()
                print(predict)
                result = predict.predictRow(data)
                return Response('Prediction is: ' + str(json.loads(result)))
        else:
            print('Nothing Passed')
    except ValueError:
        return Response('Error Occured! %s' %str(ValueError))

    except KeyError :
        return Response('Error Occured! %s' %str(KeyError))

    except Exception as e:
        return Response('Error Occured! %s' %str(e))





@app.route("/predictRow",methods=['POST'])

def predictRow():
    
    try:
        if request.form is not None:
            if request.form['datetime'] is not None and request.form['windspeed'] is not None and request.form['atmospherictemperature'] is not None and request.form['shafttemperature'] is not None and request.form['bladesangle'] is not None and request.form['gearboxtemperature'] is not None and request.form['enginetemperature'] is not None and request.form['motortorque'] is not None and request.form['generatortemperature'] is not None and request.form['atmosphericpressure'] is not None and request.form['areatemperature'] is not None and request.form['windmillbodytemperature'] is not None and request.form['winddirection'] is not None and request.form['resistance'] is not None and request.form['rotortorque'] is not None and request.form['turbinestatus'] is not None and request.form['cloudlevel'] is not None and request.form['bladelength'] is not None and request.form['blade_breadth'] is not None and request.form['windmillheight'] is not None:
                
                datetime  = request.form['datetime']
                wind_speed = request.form['windspeed']
                atmospheric_temperature = request.form['atmospherictemperature']
                shaft_temperature = request.form['shafttemperature']
                blades_angle= request.form['bladesangle']
                gearbox_temperature= request.form['gearboxtemperature']
                engine_temperature= request.form['enginetemperature']
                motor_torque = request.form['motortorque']
                generator_temperature = request.form['generatortemperature']
                atmospheric_pressure = request.form['atmosphericpressure']
                area_temperature= request.form['areatemperature']
                windmill_body_temperature = request.form['windmillbodytemperature']
                wind_direction = request.form['winddirection']
                resistance = request.form['resistance']
                rotor_torque = request.form['rotortorque']
                turbine_status = request.form['turbinestatus']
                cloud_level = request.form['cloudlevel']
                blade_lengt= request.form['bladelengt']
                blade_breadth = request.form['bladebreadth']
                windmill_height = request.form['windmillheight']
                

                data = pd.DataFrame(
                    [[datetime,wind_speed,atmospheric_temperature,shaft_temperature,blades_angle,gearbox_temperature,engine_temperature,motor_torque,generator_temperature,atmospheric_pressure,area_temperature,windmill_body_temperature,wind_direction,resistance,rotor_torque,turbine_status,cloud_level,blade_lengt,blade_breadth,windmill_height]],
                    columns=['datetime', 'wind_speed(m/s)','atmospheric_temperature(°C)', 'shaft_temperature(°C)','blades_angle(°)', 'gearbox_temperature(°C)', 'engine_temperature(°C)','motor_torque(N-m)', 'generator_temperature(°C)','atmospheric_pressure(Pascal)', 'area_temperature(°C)','windmill_body_temperature(°C)', 'wind_direction(°)', 'resistance(ohm)','rotor_torque(N-m)', 'turbine_status', 'cloud_level', 'blade_length(m)','blade_breadth(m)', 'windmill_height(m)'])
                
                predict =Prediction_Row()
                
                result = predict.predictRow(data)
                return Response('Prediction is: ' + str(json.loads(result)))
        else:
            print('Nothing Passed')
    except ValueError:
        return Response('Error Occured! %s' %str(ValueError))

    except KeyError :
        return Response('Error Occured! %s' %str(KeyError))

    except Exception as e:
        return Response('Error Occured! %s' %str(e))