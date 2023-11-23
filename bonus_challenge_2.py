import pandas as pd
import numpy as np

def dataframe_importer():
    url = input("Please enter the csv url: ")
    column_names = ["Customer","ST","GENDER","Education","Customer Lifetime Value",
                "Income","Monthly Premium Auto","Number of Open Complaints","Policy Type",
                "Vehicle Class","Total Claim Amount"]
    df = pd.read_csv(url, usecols=column_names)
    return df
def column_renamer(df):
    df.columns = [x.lower() for x in df.columns]
    df.columns = df.columns.str.replace(' ', '_')
    df.rename(columns={"st":"state"}, inplace=True)
    return df

def invalid_value_cleaner(df):
    gender_dict = {'Femal':'F', 'female':'F', 'Male':'M'}
    df['gender'] = df['gender'].replace(gender_dict)
    state_dict = {'AZ':'Arizona', 'Cali':'California', 'WA':'Washington'}
    df['state'] = df['state'].replace(state_dict)
    df['education'] = df['education'].replace({'Bachelors':'Bachelor'})
    car_dict = {'Sports Car':'Luxury', 'Luxury SUV':'Luxury', 'Luxury Car':'Luxury'}
    df['vehicle_class'] = df['vehicle_class'].replace(car_dict)
    df['customer_lifetime_value'] = df['customer_lifetime_value'].str.replace('%','')
    df['customer_lifetime_value'].head()
    return df

def datatype_formatter(df):
    df['customer_lifetime_value'] = df['customer_lifetime_value'].astype(float)
    df['number_of_open_complaints'] = df['number_of_open_complaints'].str[2:-3]
    return df

def null_value_method(df):
    df['gender'] = df['gender'].fillna(df['gender'].mode()[0])
    df = df.dropna()
    #df['number_of_open_complaints'] = df['number_of_open_complaints'].astype(int)
    return df

def duplicated_formatter(df):
    df = df.loc[df.duplicated() == False]
    return df

def column_cleaner_pipeline():
    df = dataframe_importer()
    df = column_renamer(df)
    df = invalid_value_cleaner(df)
    df = datatype_formatter(df)
    df = null_value_method(df)
    df = duplicated_formatter(df)
    return df