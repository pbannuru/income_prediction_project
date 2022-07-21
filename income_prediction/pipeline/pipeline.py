import os,sys
from collections import namedtuple
from datetime import datetime
import uuid
from pyexpat import model
from income_prediction.component.data_transformation import DataTransformation
from income_prediction.logger import logging
from income_prediction.exception import IncomeException
from income_prediction.entity.entity_config import DataIngestionConfig,DataValidationConfig,DataTransformationConfig,\
    ModelTrainerConfig,ModelEvaluationConfig
from income_prediction.component.data_ingestion import DataIngestion
from income_prediction.component.data_validation import DataValidation 
from income_prediction.component.data_transformation import DataTransformation
from income_prediction.component.model_trainer import ModelTrainer
from income_prediction.component.model_evaluation import ModelEvaluation
from income_prediction.config.configuration import  Configuration
from income_prediction.entity.artifact_entity import DataIngestionArtifact, DataTransformationArtifact,DataValidationArtifact, ModelTrainerArtifact,\
ModelTrainerArtifact,ModelEvaluationArtifact
import pandas as pd

class Pipeline:
    
    def __init__(self,config: Configuration = Configuration()) -> None:
        try:
            self.config=config

        except Exception as e:
            raise IncomeException(e,sys) from e

    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise IncomeException(e,sys) from e
    def start_data_validation(self,data_ingestion_artifact:DataIngestionArtifact) \
        -> DataValidationArtifact :
        try:
            data_validation =  DataValidation(data_validation_config=self.config.get_data_validation_config(),
                                              data_ingestion_artifact=data_ingestion_artifact
            )
            return data_validation.initiate_data_validation()
        except Exception as e:
            raise IncomeException(e,sys) from e   
    def start_data_transformation(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_artifact:DataValidationArtifact)\
        ->DataTransformationArtifact:
            try:
                data_transformation=DataTransformation(data_transformation_config=self.config.get_data_transformation_config(),
                                                       data_ingestion_artifact=data_ingestion_artifact,
                                                       data_validation_artifact=data_validation_artifact) 
                return data_transformation.initiate_data_transformation()
            except Exception as e:
                raise IncomeException(e,sys) from e  
    def start_model_trainer(self, data_transformation_artifact: DataTransformationArtifact) -> ModelTrainerArtifact:
        try:
            model_trainer = ModelTrainer(model_trainer_config=self.config.get_model_trainer_config(),
                                         data_transformation_artifact=data_transformation_artifact
                                         )
            return model_trainer.initiate_model_trainer()
        except Exception as e:
            raise IncomeException(e, sys) from e     
    def start_model_evaluation(self, data_ingestion_artifact: DataIngestionArtifact,
                               data_validation_artifact: DataValidationArtifact,
                               model_trainer_artifact: ModelTrainerArtifact) -> ModelEvaluationArtifact:
        try:
            model_eval = ModelEvaluation(
                model_evaluation_config=self.config.get_model_evaluation_config(),
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_artifact=data_validation_artifact,
                model_trainer_artifact=model_trainer_artifact)
            return model_eval.initiate_model_evaluation()
        except Exception as e:
            raise IncomeException(e, sys) from e

    
                
    def run_pipeline(self):
        try:
            if Pipeline.experiment.running_status:
                logging.info("Pipeline is already running")
                return Pipeline.experiment
            # data ingestion
            logging.info("Pipeline starting.")

            #data ingestion

            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact=self.start_data_transformation(data_ingestion_artifact,data_validation_artifact)
            model_trainer_artifact=self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            model_evaluation_aritfact=self.start_model_evaluation(data_ingestion_artifact=data_ingestion_artifact,
                                                                  data_validation_artifact=data_validation_artifact,
                                                                  model_trainer_artifact=model_trainer_artifact)

        except Exception as e:
            raise IncomeException(e,sys) from e

    def run(self):
        try:
            self.run_pipeline()
        except Exception as e:
            raise e