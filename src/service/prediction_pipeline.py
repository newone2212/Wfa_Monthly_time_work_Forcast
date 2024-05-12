import os
import pandas as pd
import numpy as np
from datetime import timedelta
from src.utils.utils import  load_object
from src.db.conn import getData,writeToDatabase
from src.utils.utils import generate_future_dates

def ProcessingModel(df):
    try:
        df['total_amount'] = df['total_amount'].astype(float)
        df['payment_month'] = pd.to_datetime(df['payment_month'])
        
        print(df.info())
        print(df.head(5))

        q1 = np.quantile(df['total_amount'], 0.25)
        q3 = np.quantile(df['total_amount'], 0.75)
        IQRamount = q3 - q1
        print(IQRamount)
       
        # Keep track of indices to drop
        indices_to_drop = []
       
        for i in range(len(df['total_amount'])):
            if df['total_amount'][i] < q1 - 3 * IQRamount or df['total_amount'][i] > q3 + 3 * IQRamount:
                indices_to_drop.append(i)
       
        # Remove outliers from the DataFrame
        df.drop(df.index[indices_to_drop], inplace=True)
       
        # Extract the target variable
        y = df['total_amount']
 
        model = load_object()
       
        model.fit(df['total_amount'])
        print("hii")
        last_date = df['payment_month'].max()
        last_date_str = last_date.strftime("%Y-%m")
        print(last_date_str)

        # Forecast future values
        future_steps = 24
        forecast, conf_int = model.predict(n_periods=future_steps, return_conf_int=True)
 
        # Generate future dates for the next 2 months
        forecast_dates = generate_future_dates(last_date_str, future_steps=future_steps, gap_days=30)
        forecast_data = []

        for forecast, conf_int in zip(forecast, conf_int):
            forecast_data.append(forecast)
        
        print(forecast_data) 
        print(forecast_dates)
        
        # Generate predicted values for the observed period
        predicted_values = model.predict_in_sample()
        print(predicted_values)
       
        # Create a new column 'predicted_amount' and assign the predicted values
        df['predicted_amount'] = predicted_values
        
        
        # Print the DataFrame after processing
        print(df.head(5))
        
        # create a new data frame for forecasting
        df_fore = pd.DataFrame({'forecast_dates': forecast_dates, 'forecast_amount': forecast_data})
        print(df_fore.head(2))
        df_fore['forecast_dates'] = pd.to_datetime(df_fore['forecast_dates'], format='%Y-%m')
        
        return df_fore, df
    except Exception as e:
        print(f"An error occurred during preprocessing: {e}")
        return pd.DataFrame(), pd.DataFrame()



def PredictionPipeline():
    try:
        df = getData()  # Assuming getData() retrieves the data
        print(df.tail(5))

        print(df.head(5))
        print("Ram")
        
        # Process data and obtain forecast DataFrame
        Forecast_df, df = ProcessingModel(df)
        
        print("siya")
        print(Forecast_df.head(2))
        
        # Write prediction data to the database
        data1 = writeToDatabase(df, "Nexum_PayAmount_Prediction")
        
        # Check if prediction data was written successfully
        if data1 == "Processed successfully":
            # Write forecast data to the database only if prediction data was written successfully
            data2 = writeToDatabase(Forecast_df, "Nexum_PayAmount_Forecast")
            if data2 == "Processed successfully":
                print("Both prediction and forecast data were written to the database successfully.")
                print("Ram")
                return "Prediction Performed Successfully"
            else:
                print("Error: Forecast data was not written to the database successfully.")
                return "Forecast data insertion failed"
        else:
            print("Error: Prediction data was not written to the database successfully.")
            return "Prediction data insertion failed"
    except Exception as e:
        print(f"An error occurred: {e}")
        return "Prediction Failed"