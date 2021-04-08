"""
The interface for data preprocessing.

Authors:
    LogPAI Team

"""

# Standard Library
import json

# Third Party
import numpy as np

# from scipy.special import expit


class FeatureExtractor(object):
    def __init__(self):
        self.idf_vec = None
        self.mean_vec = None
        self.events = None
        self.term_weighting = None
        self.normalization = None

    def save(self, filename="preprocess_model.json"):
        with open(filename, "w") as fout:
            out_dict = {}
            out_dict["events"] = self.events
            out_dict["normalization"] = self.normalization
            out_dict["term_weighting"] = self.term_weighting
            if self.idf_vec is not None:
                out_dict["idf_vec"] = self.idf_vec.tolist()
            if self.mean_vec is not None:
                out_dict["mean_vec"] = self.mean_vec.tolist()

            json.dump(out_dict, fout)

    def load(self, filename="preprocess_model.json"):
        with open(filename, "r") as fin:
            out_dict = json.load(fin)
            if "idf_vec" in out_dict:
                self.idf_vec = np.array(out_dict["idf_vec"])
            if "mean_vec" in out_dict:
                self.mean_vec = np.array(out_dict["mean_vec"])
            self.events = out_dict["events"]
            self.normalization = out_dict["normalization"]
            self.term_weighting = out_dict["term_weighting"]

    def fit_transform(self, X_seq, term_weighting=None, normalization=None):
        """Fit and transform the data matrix

        Arguments
        ---------
            X_seq: ndarray, log sequences matrix
            term_weighting: None or `tf-idf`
            normalization: None or `zero-mean`
            oov: bool, whether to use OOV event
            min_count: int, the minimal occurrence of events (default 0), only valid when oov=True.

        Returns
        -------
            X_new: The transformed data matrix
        """
        X = np.array(X_seq)
        self.normalization = normalization
        self.term_weighting = term_weighting

        num_instance, num_event = X.shape
        if self.term_weighting == "tf-idf":
            df_vec = np.sum(X > 0, axis=0)
            self.idf_vec = np.log(num_instance / (df_vec + 1e-8))
            idf_matrix = X * np.tile(self.idf_vec, (num_instance, 1))
            X = idf_matrix
        if self.normalization == "zero-mean":
            mean_vec = X.mean(axis=0)
            self.mean_vec = mean_vec.reshape(1, num_event)
            X = X - np.tile(self.mean_vec, (num_instance, 1))
        # elif self.normalization == 'sigmoid':
        #    X[X != 0] = expit(X[X != 0])
        X_new = X

        # print('Train data shape: {}-by-{}\n'.format(X_new.shape[0], X_new.shape[1]))
        return X_new

    def transform(self, X_seq):
        """Transform the data matrix with trained parameters

        Arguments
        ---------
            X: log sequences matrix
            term_weighting: None or `tf-idf`

        Returns
        -------
            X_new: The transformed data matrix
        """
        X = np.array(X_seq)

        num_instance, num_event = X.shape
        if self.term_weighting == "tf-idf":
            idf_matrix = X * np.tile(self.idf_vec, (num_instance, 1))
            X = idf_matrix
        if self.normalization == "zero-mean":
            X = X - np.tile(self.mean_vec, (num_instance, 1))
        # elif self.normalization == 'sigmoid':
        #    X[X != 0] = expit(X[X != 0])
        X_new = X

        # print('Test data shape: {}-by-{}\n'.format(X_new.shape[0], X_new.shape[1]))

        return X_new
