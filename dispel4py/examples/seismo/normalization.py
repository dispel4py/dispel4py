# -*- coding: utf-8 -*-
"""
Created on Mon Apr 07 09:23:16 2014

@author: abell5
"""
from scipy.signal import triang
from numpy import sign, arange, zeros, absolute, true_divide, sum,  floor, convolve, amax, logical_and
import copy

def onebit_norm(stream):
    stream2 = copy.deepcopy(stream)
    
    for trace in arange(len(stream2)):
        data = stream2[trace].data
        data = sign(data)
        stream2[trace].data = data
        
    return stream2


def mean_norm(stream,N):
    stream2 = copy.deepcopy(stream)
    
    for trace in arange(len(stream2)):
        data = stream2[trace].data
        
        w = zeros(len(data)) 
        naux = zeros(len(data))
        
        for n in arange(len(data)):
            if n<N:
                tw = absolute(data[0:n+N])
            elif logical_and(n>=N, n<(len(data)-N)):
                tw = absolute(data[n-N:n+N])
            elif n>=(len(data)-N):
                tw = absolute(data[n-N:len(data)])
                
            w[n]=true_divide(1,2*N+1)*(sum(tw))
            
        naux=true_divide(data,w)
        stream2[trace].data = naux
        
    return stream2


def gain_norm(stream, N):
    stream2 = copy.deepcopy(stream)
    
    for trace in arange(len(stream2)):
        data = stream2[trace].data
        
        dt = 1./(stream2[trace].stats.sampling_rate)
        L = floor((N/dt+1./2.))
        h = triang(2.*L+1.)
        
        e = data**2.
       
        rms = (convolve(e,h,'same')**0.5)
        epsilon = 1.e-12*amax(rms)
        op = rms/(rms**2+epsilon)
        dout = data*op
        
        stream2[trace].data = dout
        
    return stream2
