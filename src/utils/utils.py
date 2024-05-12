import os
import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).parent.parent))
import dill
# import pickle4 as pickle
import datetime

def generate_future_dates(start_date, future_steps, gap_days):
    dates = []
    current_date = datetime.datetime.strptime(start_date, "%Y-%m") + datetime.timedelta(days=gap_days)
    
    for _ in range(future_steps):
        dates.append(current_date.strftime("%Y-%m"))
        current_date += datetime.timedelta(days=gap_days)
        
    return dates



def save_object(file_path, obj):
    try:
        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)

    except Exception as e:
        raise str(e)
    
    
def load_object():
    try:
        print("Entered the load_object")
        model_path = "artifacts/amountmodel.pkl"
        print(model_path)
        with open(model_path, "rb") as file_obj:
            print("hii")
            loaded_model = dill.load(file_obj)
            print("Model Loaded")
        return loaded_model
    except FileNotFoundError as e:
        print("Model file not found at path:", model_path)
        return None
    except Exception as e:
        print("Error loading model:", str(e))
        return None