# -*- coding: utf-8 -*-
"""
Created on Mon Apr 07 09:32:54 2014

@author: abell5
"""

from obspy.signal.util import nextpow2
from scipy.fftpack import fft, ifft
from numpy import complex, conjugate, roll, copy
import scipy.signal
import scipy.fftpack
import numpy
from obspy.signal.cross_correlation import xcorr

'''
gapcorr = 0 fill gaps with 0s
gapcorr = 1 fill gaps with interpolation

tlength: time length of raw data in seconds
tlength = 86400 for Laquila dataset
'''

def xcorrf(trace1, trace2, shift=None):
    """
    Cross-correlation of numpy arrays data1 and data2 in frequency domain.
    """
    data1 = trace1.data
    data2 = trace2.data
    
    complex_result = (data1.dtype == complex or data2.dtype == complex)
    N1 = len(data1)
    N2 = len(data2)

    data1 = data1.astype('float64')
    data2 = data2.astype('float64')

    # Always use 2**n-sized FFT, perform xcorr
    size = max(2 * shift + 1, (N1 + N2) // 2 + shift)
    nfft = nextpow2(size)
    IN1 = fft(data1, nfft)
    IN1 *= conjugate(fft(data2, nfft))
    ret = ifft(IN1)
    del IN1
    
    if not complex_result:
        ret = ret.real
    # shift data for time lag 0 to index 'shift'

    ret = roll(ret, -(N1 - N2) // 2 + shift)[:2 * shift + 1]

    return copy(ret)

'''
Mario's attemp at the XCorr PE
The traces are normalized and calculate xcorr with obspy.signal.cross_correlation
author mdavid@ipgp.fr
'''
def PEXCorr1(st1, st2, maxlag):
    st1 = st1/numpy.linalg.norm(st1)
    st2 = st2/numpy.linalg.norm(st2)
    return xcorr(st1, st2, maxlag, full_xcorr=True)[2]

'''
This one adapted from MSNoise - NOT WORKING properly at the moment
MSNoise is a joint project of the Royal Observatory of Belgium (Thomas Lecocq and Corentin Caudron) and ISTerre + IPGP (Florent Brenguier)
http://www.msnoise.org/
'''
def PEXCorr2(st1, st2, maxlag):
    """
    This function takes ndimensional *data* array, computes the cross-correlation in the frequency domain
    and returns the cross-correlation function between [-*maxlag*:*maxlag*].
    !add a line on the +++++----- to -----++++++

    :param numpy.ndarray data: This array contains the fft of each timeseries to be cross-correlated.
    :param int maxlag: This number defines the number of samples (N=2*maxlag + 1) of the CCF that will be returned.

    :rtype: numpy.ndarray
    :returns: The cross-correlation function between [-maxlag:maxlag]
"""

    fft1 = scipy.fftpack.fft(st1)
    fft2 = scipy.fftpack.fft(st1)
    data = numpy.array([fft1, fft2])

    normalized = True

    # maxlag = numpy.round(maxlag)
    #~ print "np.shape(data)",np.shape(data)
    if numpy.shape(data)[0] == 2:
        K = numpy.shape(data)[0]
        #couples de stations
        couples = numpy.concatenate((numpy.arange(0, K), K + numpy.arange(0, K)))

    Nt = numpy.shape(data)[1]
    Nc = 2 * Nt - 1

    # next power of 2
    Nfft = 2 ** numpy.ceil( numpy.log2(numpy.abs(Nc)) )

    # corr = scipy.fftpack.fft(data,int(Nfft),axis=1)
    corr = data
    corr = numpy.conj(corr[couples[0]]) * corr[couples[1]]
    corr = numpy.real(scipy.fftpack.ifft(corr)) / Nt
    corr = numpy.concatenate((corr[-Nt + 1:], corr[:Nt + 1]))
    E = numpy.sqrt(numpy.mean(scipy.fftpack.ifft(data, axis=1) ** 2, axis=1))
    normFact = E[0] * E[1]
    if normalized:
        corr /= numpy.real(normFact)
    if maxlag != Nt:
        tcorr = numpy.arange(-Nt + 1, Nt)
        dN = numpy.where(numpy.abs(tcorr) <= maxlag)[0]
        corr = corr[dN]
    del data
    return corr

################################################
# Codes developed for the Whisper Project,
# FP7 ERC Advanced grant 227507
# by Xavier Briand: xav.briand.whisper@gmail.com
# with Michel Campillo and Philippe Roux.
################################################

