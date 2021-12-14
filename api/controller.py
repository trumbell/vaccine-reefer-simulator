
from flask import Blueprint, request, abort, jsonify
import logging, os
from flasgger import swag_from
from flask_restful import Resource, Api
from concurrent.futures import ThreadPoolExecutor
from api.prometheus import track_requests
from infrastructure.MetricsEventsProducer import MetricsEventsProducer 
from domain.reefer_simulator import ReeferSimulator
import logging
import datetime


"""
 created a new instance of the Blueprint class and bound the Controller resource to it.
"""

control_blueprint = Blueprint("control", __name__)
api = Api(control_blueprint)

class SimulationController(Resource):

    def __init__(self):
        print("SimulationController")
        self.simulator = ReeferSimulator()
        self.metricsProducer = MetricsEventsProducer()

    @swag_from('version.yaml')
    def get(self):
        return jsonify({"version": "v0.0.4"})
    

    def sendEvents(self,metrics):
        """
        Send Events to Kafka
        """
        logging.info(metrics)
        for metric in metrics:
            print(metric)
            evt = {"containerID": metric['container_id'],
                    "eventLogTime": str(datetime.datetime.now()),
                    "timestamp": str(metric['measurement_time']),
                    "type":"ReeferTelemetries",
                    "payload": metric}
            self.metricsProducer.publishEvent(evt,"containerID")
            
    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('controlapi.yml')
    def post(self):
        logging.info("post control received: ")
        control = request.get_json(force=True)
        logging.info(control)
        if not 'containerID' in control:
            abort(400) 

        nb_records = int(control["nb_of_records"])

        if 'nb_in_batch' in control:
            nb_batch = int(control["nb_in_batch"])

            # metrics_list = []
            for container_name in [f'C{i:06}' for i in range(1,nb_batch)]:
                if control["simulation"] == ReeferSimulator.SIMUL_POWEROFF:
                    metrics=self.simulator.generatePowerOff(container_name,nb_records,control["product_id"])
                elif  control["simulation"]  == ReeferSimulator.SIMUL_CO2:
                    metrics=self.simulator.generateCo2(container_name,nb_records,control["product_id"])
                elif  control["simulation"]  == ReeferSimulator.SIMUL_O2:
                    metrics=self.simulator.generateO2(container_name,nb_records,control["product_id"])
                elif  control["simulation"]  == ReeferSimulator.SIMUL_TEMPERATURE:
                    metrics=self.simulator.generateTemperature(container_name,nb_records,control["product_id"])
                elif  control["simulation"]  == ReeferSimulator.NORMAL:
                    metrics=self.simulator.generateNormal(container_name,nb_records,control["product_id"])
                elif  control["simulation"]  == ReeferSimulator.TEMP_GROWTH:
                    metrics=self.simulator.generateTemperatureGrowth(container_name,nb_records,control["product_id"])
                else:
                    return {"error":"Wrong simulation controller data"},404
                # metrics_list.append(metrics)
                self.sendEvents(metrics)
                # metrics = metrics_list
        else:
            if control["simulation"] == ReeferSimulator.SIMUL_POWEROFF:
                metrics=self.simulator.generatePowerOff(control["containerID"],nb_records,control["product_id"])
            elif  control["simulation"]  == ReeferSimulator.SIMUL_CO2:
                metrics=self.simulator.generateCo2(control["containerID"],nb_records,control["product_id"])
            elif  control["simulation"]  == ReeferSimulator.SIMUL_O2:
                metrics=self.simulator.generateO2(control["containerID"],nb_records,control["product_id"])
            elif  control["simulation"]  == ReeferSimulator.SIMUL_TEMPERATURE:
                metrics=self.simulator.generateTemperature(control["containerID"],nb_records,control["product_id"])
            elif  control["simulation"]  == ReeferSimulator.NORMAL:
                metrics=self.simulator.generateNormal(control["containerID"],nb_records,control["product_id"])
            elif  control["simulation"]  == ReeferSimulator.TEMP_GROWTH:
                metrics=self.simulator.generateTemperatureGrowth(control["containerID"],nb_records,control["product_id"])
            else:
                return {"error":"Wrong simulation controller data"},404
        
            self.sendEvents(metrics)
            
        return metrics,202
    


api.add_resource(SimulationController, "/control")