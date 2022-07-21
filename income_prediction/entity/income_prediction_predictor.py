import os
import sys

from income_prediction.exception import IncomeException
from income_prediction.util.util import load_object

import pandas as pd


class income_prediction_Data:

    def __init__(self,
                 age: int,
                 workclass: object,
                 fnlwgt: int, 
                 education_num: int, 
                 marital_status: object,
                 occupation: object,
                 relationship: object,
                 race: object,
                 sex: object,
                 capital_gain: int, 
                 capital_loss: int, 
                 hours_per_week: int, 
                 native_country: object,
                 wages: object

                 ):
        try:
            self.age = age
            self.workclass = workclass
            self.fnlwgt = fnlwgt
            self.education_num = education_num
            self.marital_status = marital_status
            self.occupation =  occupation
            self. race =  race
            self.sex = sex
            self.capital_gain = capital_gain
            self.capital_loss = capital_loss
            self.hours_per_week = hours_per_week
            self.native_country = native_country
            self.wages = wages
        except Exception as e:
            raise IncomeException(e, sys) from e

    def get_income_prediction_input_data_frame(self):

        try:
            housing_input_dict = self.get_income_prediction_data_as_dict()
            return pd.DataFrame(housing_input_dict)
        except Exception as e:
            raise IncomeException(e, sys) from e

    def get_income_prediction_data_as_dict(self):
        try:
            input_data = {
                "age": [self.age],
                "workclass":[self.workclass],
                "fnlwgt": [self.fnlwgt],
                "education_num ": [self.education_num],
                "marital_status": [self.marital_status],
                "occupation": [self.occupation],
                "race": [self. race], 
                "sex": [self.sex], 
                "capital_gain": [self.capital_gain], 
                "capital_loss": [self.capital_loss], 
                "hours_per_week": [self.hours_per_week], 
                "native_country": [self.native_country], 
                "wages": [self.wages]
                }
            return input_data
        except Exception as e:
            raise IncomeException(e, sys)


class income_prediction_Predictor:

    def __init__(self, model_dir: str):
        try:
            self.model_dir = model_dir
        except Exception as e:
            raise IncomeException(e, sys) from e

    def get_latest_model_path(self):
        try:
            folder_name = list(map(int, os.listdir(self.model_dir)))
            latest_model_dir = os.path.join(self.model_dir, f"{max(folder_name)}")
            file_name = os.listdir(latest_model_dir)[0]
            latest_model_path = os.path.join(latest_model_dir, file_name)
            return latest_model_path
        except Exception as e:
            raise IncomeException(e, sys) from e

    def predict(self, X):
        try:
            model_path = self.get_latest_model_path()
            model = load_object(file_path=model_path)
            median_house_value = model.predict(X)
            return median_house_value
        except Exception as e:
            raise IncomeException(e, sys) from e