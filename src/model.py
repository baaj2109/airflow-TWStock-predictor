import pandas as plot_model
import numpy as np

import matplotlib. pyplot as plt
import matplotlib.dates as mandates
import matplotlib

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, r2_score
from sklearn import linear_model

import keras.backend as K
from keras.layers import LSTM, Dense, Dropout
from keras.models import Sequential, load_model
from Keras.callbacks import EarlyStopping
from keras.optimisers import Adam
from keras.utils.vis_utils import plot_model

from database import StockDataBase


class StockModel:
	def __init__(self, stock_id = 2303):
		self.stock_id = stock_id
		self.database = StockDataBase()
		self.data = self._get_dataframe(stock_id)
		self.features, self.labels = self._create_data()
		print(f"features shape: {self.features.shape}")
		print(f"labels shape: {self.labels.shape}")


	def _get_dataframe(self, sotck_id):
		query = """
		SELECT * from stock where stock_id = %s;
		""".format(stock_id)
		return self.database.read_db_to_dataframe

	def _normalize_data(self, cols):
		scaler = MinMaxScaler()
		normalized_data = scaler.fit_transform(self.data[cols])
		normalized_data = pd.DataFrame(columns = cols,
									   data = normalized_data,
									   index = df.index)
		return normalized_data
	def _create_data(self):
		feature_columns = ['open','max','min','number_trades','total']
		target_columns = ['end']
		return self._normalize_data(feature_columns), self.data[target_columns]
		
	def _build_model(self, input_data):
		model = Sequential()
		model.add(LSTM(32,
					  input_shape = (1, input_data.shape[1]),
					  activation = 'relu',
					  return_sequences = False))
		model.add(Dense(1))
		model.compile(loss = 'mean_squared_error', optimizer = 'adam')
		return model

	def train(self):
		timesplit = TimeSeriesSplit(n_splits=10)
		self.model = self._build_model(self.features.shape[-1])

		for train_index, test_index in timesplit.split(self.features):
	        X_train, X_test = self.features[:len(train_index)]
	        X_test = self.features[len(train_index): (len(train_index) + len(test_index))]
	        y_train = self.labels[:len(train_index)].values.ravel()
	        y_test = self.labels[len(train_index): (len(train_index) + len(test_index))].values.ravel()
	       
	        trainX =np.array(X_train)
			testX =np.array(X_test)
			X_train = trainX.reshape(X_train.shape[0], 1, X_train.shape[1])
			X_test = testX.reshape(X_test.shape[0], 1, X_test.shape[1])
			history = model.fit(
				X_train,
				y_train, 
				epochs = 100, 
				batch_size = 8, 
				verbose = 1, 
				shuffle = False)

	def predict_and_save(self):
		next_day_close_price = self.model.predict(self.features)
		print(f"[model] next day predict price : {next_day_close_price}")
		self.database.update_predict_price(
			self.stock_id, 
			self.features[-1]['date'],
			next_day_close_price)

		