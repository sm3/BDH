#!/usr/bin/env python
"""
Implement your own version of logistic regression with stochastic
gradient descent.

Author: <your_name>
Email : <your_email>
"""

import collections
import math



class LogisticRegressionSGD:

    def __init__(self, eta, mu, n_feature):
        """
        Initialization of model parameters
        """
        self.eta = eta
        self.weight = [0.0] * n_feature
        self.features = n_feature

        self.mu = mu if mu else 0.0



    def fit(self, X, y):
        """
        Update model using a pair of training sample
        """





        grd = self.predict_prob(X)-y# gradient
        #update weights under the penalty of L2 norm regularization.
        #formula #3 from http://www.hongliangjie.com/notes/lr.pdf


        for f, v in X:
            self.weight[f] -= self.eta * grd +(self.mu*self.weight[f])
            #self.weight[f] -= self.eta * grd

    def predict(self, X):
        return 1 if self.predict_prob(X) > 0.5 else 0

    def predict_prob(self, X):
        return 1.0 / (1.0 + math.exp(-math.fsum((self.weight[f]*v for f, v in X))))
