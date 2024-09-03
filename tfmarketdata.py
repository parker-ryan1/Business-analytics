import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

# Step 1: Download historical stock data from Yahoo Finance
ticker = 'jpm'  # Example: Apple Inc.
stock_data = yf.download(ticker, start='2015-01-01', end='2023-08-01')

# Step 2: Preprocess the data
close_prices = stock_data['Close'].values
close_prices = close_prices.reshape(-1, 1)

# Scale the data to be between 0 and 1
scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(close_prices)

# Create sequences (X_train) and the corresponding labels (y_train)
sequence_length = 60  # Use the past 60 days to predict the next day
X_train = []
y_train = []

for i in range(sequence_length, len(scaled_data)):
    X_train.append(scaled_data[i-sequence_length:i, 0])
    y_train.append(scaled_data[i, 0])

X_train, y_train = np.array(X_train), np.array(y_train)
X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))

# Step 3: Build the LSTM model
model = Sequential()
model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)))
model.add(Dropout(0.2))
model.add(LSTM(units=50, return_sequences=False))
model.add(Dropout(0.2))
model.add(Dense(units=25))
model.add(Dense(units=1))

# Compile the model
model.compile(optimizer='adam', loss='mean_squared_error')

# Step 4: Train the model
model.fit(X_train, y_train, epochs=5, batch_size=32)

# Step 5: Predict the next 6 months (180 days)
test_data = scaled_data[-sequence_length:]
predictions = []

for _ in range(180):  # Predicting for the next 180 days
    X_test = np.reshape(test_data, (1, sequence_length, 1))
    predicted_price = model.predict(X_test)
    predictions.append(predicted_price[0, 0])
    test_data = np.append(test_data, predicted_price, axis=0)
    test_data = test_data[1:]  # Slide the window forward

# Inverse transform the predictions to original scale
predictions = scaler.inverse_transform(np.array(predictions).reshape(-1, 1))

# Step 6: Plot the results
# Step 6: Plot the results
predicted_dates = pd.date_range(start=stock_data.index[-1], periods=181, inclusive='right')
predicted_series = pd.Series(predictions.flatten(), index=predicted_dates)

plt.figure(figsize=(14, 7))
plt.plot(stock_data.index, close_prices, label='Historical Close Price', color='blue')
plt.plot(predicted_series.index, predicted_series, label='Predicted Close Price', color='red', linestyle='--')
plt.axvline(x=stock_data.index[-1], color='gray', linestyle='--', label='Prediction Start')
plt.title(f'{ticker} Stock Price Prediction for the Next 6 Months')
plt.xlabel('Date')
plt.ylabel('Stock Price (USD)')
plt.legend()
plt.grid(True)
plt.show()
