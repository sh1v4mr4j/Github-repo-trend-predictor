import os
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from sklearn.preprocessing import MinMaxScaler
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import mean_squared_error  # for regression tasks
import numpy as np

class SimpleNN(nn.Module):
    def __init__(self):
        super(SimpleNN, self).__init__()
        self.fc1 = nn.Linear(4, 32)  # Assuming 3 input features
        self.fc2 = nn.Linear(32, 64)
        self.fc3 = nn.Linear(64, 32) 
        self.fc4 = nn.Linear(32, 1) 

    def forward(self, x):
        x = torch.relu(self.fc1(x))  # ReLU activation for hidden layers
        x = torch.relu(self.fc2(x))
        x = torch.relu(self.fc3(x))
        x = self.fc4(x)
        return x


class CSVHandler(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.processed_count = 0
        self.train_file = "train.csv"
        self.val_file = "validation.csv"
        self.test_file = "test.csv"

    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            print(f"New CSV detected: {event.src_path}")
            self.process_csv(event.src_path)

    def process_csv(self, file_path):
        try:
            # Read the CSV file
            data = pd.read_csv(file_path)
            print(f"Processing CSV file:\n{data.head()}")  # Example: print first 5 rows

            #initialise the model
            model = SimpleNN()

            #preprocess the data
            dataset = self.pre_process(data)


            if self.processed_count == 0:
                print(f"Processing to get unique 400 data logic:\n{data.head()}")  # Custom logic for the first 3 files
                self.add_to_train(dataset)


            if self.processed_count == 1:
                print("Creating validation dataset")
                self.add_to_validation(dataset)


            if self.processed_count == 2:
                print("Getting training data and validation data and normalising")
                train_data = pd.read_csv(self.train_file)
                validation_data = pd.read_csv(self.val_file)

                train_len = len(train_data)

                print("Combine the data for normalization")
                combined_data = pd.concat([train_data, validation_data])

                print("Normalising...")
                combined_data = self.normalise(combined_data)

                n_train = combined_data.iloc[:train_len].reset_index(drop=True)
                n_validation = combined_data.iloc[train_len:].reset_index(drop=True)

                print("Training the model...")
                self.train_the_model(n_train,n_validation, model)
                self.processed_count = 3
                print("Model is trained!!!")
                print("Ready to predict the repo trend...")
            elif self.processed_count == 3:
                print("Trend predictor running ...")
                self.add_to_test(dataset)
                self.predict_the_trend(self,model)
            else:
                print(f"Processing remaining files logic:\n{data.head()}")  # Custom logic for the rest
                self.do_something_with_rest(data)


        except Exception as e:
            print(f"Failed to process {file_path}: {e}")



    def pre_process(self, dataset):
        dataset['Description'] = dataset['Description'].fillna('')
        dataset['description_length'] = dataset['Description'].apply(len)

        columns_to_drop = ['Name', 'FullName', 'HtmlUrl', 'Description', 'Language','OwnerLogin', 'OwnerType', 'CreatedAt', 'UpdatedAt', 'PushedAt', 'License','WatchersCount']

        dataset = dataset.drop(columns=columns_to_drop)

        return dataset
    

    
    def add_to_train(self, dataset):
        try:
            if os.path.exists(self.train_file):
                train_data = pd.read_csv(self.train_file)
            else:
                train_data = pd.DataFrame()

            combined_data = pd.concat([train_data, dataset]).drop_duplicates()

            # Limit to 400 unique rows
            if len(combined_data) > 400:
                combined_data = combined_data.iloc[:400]
                self.processed_count = 1
                combined_data.to_csv(self.train_file, index=False)
                print("Train.csv has reached its maximum capacity of 400 unique rows.")
        except Exception as e:
            print(f"Error updating {self.train_file}: {e}")



    def add_to_validation(self,dataset):
        try:
            if os.path.exists(self.val_file):
                val_data = pd.read_csv(self.val_file)
            else:
                val_data = pd.DataFrame()

            combined_data = pd.concat([val_data, dataset]).drop_duplicates()

            # Limit to 100 unique rows
            if len(combined_data) > 100:
                combined_data = combined_data.iloc[:400]
                self.processed_count = 2
                print("Validation.csv has reached its maximum capacity of 100 unique rows.")
                combined_data.to_csv(self.val_file, index=False)
        except Exception as e:
            print(f"Error updating {self.val_file}: {e}")

    def add_to_test(self,dataset):
        try:
            test_data = pd.DataFrame()

            combined_data = pd.concat([test_data, dataset]).drop_duplicates()
            combined_data.to_csv(self.val_file, index=False)
        except Exception as e:
            print(f"Error updating {self.val_file}: {e}")



    def train_the_model(self, train_dataset, validation_dataset, model):
        try:
            scaler = MinMaxScaler()

            #Tranform training data for model
            X_train = scaler.fit_transform(train_dataset[['OpenIssuesCount', 'ForksCount', 'Size', 'description_length']])
            y_train = train_dataset['StargazersCount'].values

            X_train = torch.tensor(X_train, dtype=torch.float32)
            y_train = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)

            y_train_scaled = scaler.fit_transform(y_train.reshape(-1, 1))

            #Transform validation data for model
            X_val = scaler.fit_transform(validation_dataset[['OpenIssuesCount', 'ForksCount', 'Size', 'description_length']])
            y_val = validation_dataset['StargazersCount'].values

            X_val = torch.tensor(X_val, dtype=torch.float32)
            y_val = torch.tensor(y_val, dtype=torch.float32).view(-1, 1)

            y_val_scaled = scaler.fit_transform(y_val.reshape(-1, 1))

            #model
            criterion = nn.MSELoss()
            optimizer = optim.Adam(model.parameters(), lr=0.0001)
            
            self.train(model, X_train, y_train_scaled, X_val, y_val_scaled, criterion, optimizer, 1000)
        except Exception as e:
            print("Error in Model training :(")
        return
    
    def predict_the_trend(self,model):
        try:
            scaler = MinMaxScaler()

            test_data = pd.read_csv(self.test_file)
            n_test = self.normalise(test_data)

            #Transform testing data for model
            X_test = scaler.fit_transform(n_test[['OpenIssuesCount', 'ForksCount', 'Size', 'description_length']])
            y_test = n_test['StargazersCount'].values

            X_test = torch.tensor(X_test, dtype=torch.float32)
            y_test = torch.tensor(y_test, dtype=torch.float32).view(-1, 1)

            y_test = scaler.fit_transform(y_test.reshape(-1, 1))

            y_test_pred_scaled = model(X_test)

            y_test_pred  = scaler.inverse_transform(y_test_pred_scaled.detach().numpy())
            return


        except Exception as e:
            print("Error in predicting the trend")

    def normalise(self, dataset):
        columns_to_normalize = ['OpenIssuesCount', 'ForksCount', 'Size', 'description_length']
        data_to_normalize = dataset[columns_to_normalize]

        scaler = MinMaxScaler()
        normalized_data = scaler.fit_transform(data_to_normalize)

        dataset[columns_to_normalize] = normalized_data

        return dataset
    

    def train(self, model, X_train, y_train, X_val, y_val, optimizer, criterion, epochs):
        for epoch in range(epochs):
            model.train()
            
            # Convert to PyTorch tensors
            X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
            y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)  # Reshape to column vector
            
            optimizer.zero_grad()  # Zero gradients
            predictions = model(X_train_tensor)  # Forward pass
            loss = criterion(predictions, y_train_tensor)  # Compute loss
            
            loss.backward()  # Backward pass
            optimizer.step()  # Update weights
            
            # Validation step
            if (epoch + 1) % 100 == 0:
                model.eval()
                with torch.no_grad():
                    X_val_tensor = torch.tensor(X_val, dtype=torch.float32)
                    y_val_tensor = torch.tensor(y_val, dtype=torch.float32).view(-1, 1)
                    val_predictions = model(X_val_tensor)
                    val_loss = criterion(val_predictions, y_val_tensor)

                # Print losses for each epoch
                print(f"Epoch [{epoch+1}/{epochs}], Train Loss: {loss.item():.4f}, Val Loss: {val_loss.item():.4f}")
    

if __name__ == "__main__":
    watch_directory = "./kafka_streaming/csv_data"  # Specify your directory containing CSV files

    if not os.path.exists(watch_directory):
        os.makedirs(watch_directory)

    event_handler = CSVHandler(watch_directory)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=False)

    print(f"Monitoring directory: {watch_directory} for new CSV files...")
    try:
        observer.start()
        while True:
            time.sleep(5)  # Keeps the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
