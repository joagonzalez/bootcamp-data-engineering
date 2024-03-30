import sqlmodel
import pandas as pd
from abc import ABC, abstractmethod


class ETL(ABC):
    """
    This class implements an Extract Transform Load Workflow
    to process churn dataset and load data in a postgres db
    """
    
    @abstractmethod
    def clean_data(): pass
    
    @abstractmethod
    def create_features(): pass
    
    @abstractmethod
    def create_models(): pass
    
    abstractmethod
    def load_data(): pass



if __name__ == '__main__':
    ETL.clean_data()
    ETL.create_features()
    ETL.create_models()
    ETL.load_data()