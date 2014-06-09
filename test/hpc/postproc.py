from verce.GenericPE import GenericPE, NAME
import os

class ReadJSON(GenericPE):
    OUTPUT_NAME='output'
    def __init__(self):
        GenericPE.__init__(self)
        self.outputconnections = { ReadJSON.OUTPUT_NAME : { NAME : ReadJSON.OUTPUT_NAME } }
    def process(self, inputs):
        return { ReadJSON.OUTPUT_NAME : inputs }
    
class WatchDirectory(GenericPE):
    INPUT_NAME='input'
    OUTPUT_NAME='output'
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { WatchDirectory.INPUT_NAME : { NAME : WatchDirectory.INPUT_NAME } }
        self.outputconnections = { WatchDirectory.OUTPUT_NAME : { NAME : WatchDirectory.OUTPUT_NAME }}
    def process(self, inputs):
        for stream in inputs['input']['streams']:
            directory = pickle.loads(str(stream['data']))
            for dir_entry in os.listdir(directory):
                dir_entry_path = os.path.join(directory, dir_entry)
                if os.path.isfile(dir_entry_path):
                    self.write(WatchDirectory.OUTPUT_NAME, [ dir_entry_path ] )
               
from verce.seismo.seismo import SeismoPE, INPUT_NAME
from verce.provenance import ProvenancePE
from obspy.core import trace,stream
import numpy
from lxml import etree
from StringIO import StringIO
import pickle

class Specfem3d2Stream(SeismoPE):
    
    def num(self,s):
        try:
            return int(s)
        except ValueError:
            return float(s)

    def getDataStreams(self, inputs):
        values = inputs[INPUT_NAME]
        data = []
        for item in values:
            data.append({'data' : item})
        streams = {"streams": data}
        return streams
    
    def initParameters(self, streams):
        ProvenancePE.initParameters(self, streams)

    def produceStream(self,filepath):
        time,data=numpy.loadtxt(filepath,unpack=True)
        head,tail =  os.path.split(filepath)
        tr=trace.Trace(data)
    
        try:
            #assuming that the information are in the filename following the usual convention
            tr.stats['network']=tail.split('.')[1]
            tr.stats['station']=tail.split('.')[0]
            tr.stats['channel']=tail.split('.')[2]
     
            try:
                doc = etree.parse(StringIO(open(self.stationsFile).read()))
                ns = {"ns": "http://www.fdsn.org/xml/station/1"}
                tr.stats['latitude']= self.num(doc.xpath("//ns:Station[@code='"+tr.stats['station']+"']/ns:Latitude/text()",namespaces=ns)[0])
                tr.stats['longitude']= self.num(doc.xpath("//ns:Station[@code='"+tr.stats['station']+"']/ns:Longitude/text()",namespaces=ns)[0])
            except:
                with open(self.stationsFile) as f:
                    k=False
                    for line in f:
                
                        if (k==False):
                            k=True
                        else:
                            station={}
                            l=line.strip().split(" ")
                            if(tr.stats['station']==l[0]):
                               tr.stats['latitude']=float(l[3])
                               tr.stats['longitude']=float(l[2])  
                    
                    
        except:
            print traceback.format_exc()
#            tr.stats['network']=self.parameters["network"]
#            tr.stats['station']=self.parameters["station"]
#            tr.stats['channel']=self.parameters["channel"]
           
    
        tr.stats['starttime']=time[0]
        delta=time[1]-time[0]
        tr.stats['delta']=delta #maybe decimal here
        tr.stats['sampling_rate']=round(1./delta,1) #maybe decimal here
        if filepath.endswith('.semv'):
            tr.stats['type']="velocity"
        if filepath.endswith('.sema'):
            tr.stats['type']='acceleration'
        if filepath.endswith('.semd'):
            tr.stats['type']='displacement'
         
        self._timestamp = { 'starttime' : tr.stats['starttime'], 'endtime' : tr.stats['endtime'] }
        self._location = { 'channel' : tr.stats['channel'], 'network' : tr.stats['network'], 'station' : tr.stats['station'] }
    
        st=stream.Stream()
        st+=stream.Stream(tr)
        return st

    def compute(self):
        st=self.produceStream(self.st)
        self.outputstreams.append(st)

import numpy as np
class WavePlot_INGV(SeismoPE):
    
    import matplotlib
    matplotlib.use('Agg')
    
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdt

    def compute(self):
        self.outputdest=self.outputdest+"%s" % (self.parameters["filedestination"],);
        try:
            if not os.path.exists(self.outputdest):
                os.makedirs(self.outputdest)
        except Exception,e:
            self.error+=str(e)
        
        name=str(self.st[0].stats.network) + "." + self.st[0].stats.station + "." + self.st[0].stats.channel
        tr = self.st[0]
         
        
        try:
            if tr.stats['type']=="velocity":
                name= str(name)+".velocity"
            else:
                if tr.stats['type']=="acceleration":
                    name= str(name)+".acceleration"
                else:
                    if tr.stats['type']=="displacement":
                         name= str(name)+".displacement"
                    else:
                        name= str(name)
        except Exception, err:
            name= str(name)
        
        self.outputdest=self.outputdest+"/"+name+".png"
        date="Date: " + str(self.st[0].stats.starttime.date)
        fig = WavePlot_INGV.plt.figure()
        fig.set_size_inches(12,6)
        fig.suptitle(name)
        WavePlot_INGV.plt.figtext(0.1, 0.95,date)

        ax = fig.add_subplot(len(self.st),1,1)
        for i in xrange (len(self.st)):
            WavePlot_INGV.plt.subplot(len(self.st),1,i+1,sharex=ax)
            t=np.linspace(WavePlot_INGV.mdt.date2num(self.st[i].stats.starttime) ,
            WavePlot_INGV.mdt.date2num(self.st[i].stats.endtime) ,
            self.st[i].stats.npts)
            WavePlot_INGV.plt.plot(t, self.st[i].data,color='gray')
            ax.set_xlim(WavePlot_INGV.mdt.date2num(self.st[0].stats.starttime), WavePlot_INGV.mdt.date2num(self.st[-1].stats.endtime))
            ax.xaxis.set_major_formatter(WavePlot_INGV.mdt.DateFormatter('%I:%M %p'))
            ax.format_xdata = WavePlot_INGV.mdt.DateFormatter('%I:%M %p')

        fig1 = WavePlot_INGV.plt.gcf()
         
        WavePlot_INGV.plt.draw()
        fig1.savefig(self.outputdest)
        __file = open(self.outputdest)
        self.streamItemsLocations.append("file://"+socket.gethostname()+os.path.abspath(__file.name)) 
        self.streamItemsFormat.append("image/png")            


from scipy.cluster.vq import whiten
import socket
import traceback

class StreamToSeedFile(SeismoPE):
    
    def writeToFile(self,stream,location):
        stream.write(location,format='MSEED',encoding='FLOAT32');
        __file = open(location)
        return os.path.abspath(__file.name)
    
    def compute(self):
        
        self.outputdest=self.outputdest+"%s" % (self.parameters["filedestination"],);
        for tr in self.streams[0]:
            try:
                tr.data=tr.data.filled();
            except Exception, err:
                tr.data=np.float32(tr.data);
            
        name=str(self.streams[0][0].stats.network) + "." + self.streams[0][0].stats.station + "." + self.streams[0][0].stats.channel
        try:
            if tr.stats['type']=="velocity":
                self.outfile= str(name)+".seedv"
            else:
                if tr.stats['type']=="acceleration":
                    self.outfile= str(name)+".seeda"
                else:
                    if tr.stats['type']=="displacement":
                        self.outfile= str(name)+".seedd"
                    else:
                        self.outfile= str(name)+".seed"
        except Exception, err:
            self.outfile= str(name)+".seed"
        #self.outputdest=self.outputdest+"/"+folder
            
        try:
            if not os.path.exists(self.outputdest):
                os.makedirs(self.outputdest)
        except Exception, e:
            print "folder exists: "+self.outputdest
        self.outputdest=self.outputdest+"/"+self.outfile
            
        path=self.writeToFile(self.streams[0],self.outputdest)
        self.streamItemsLocations.append("file://"+socket.gethostname()+path)
        
        self.streamItemsFormat.append("application/octet-stream")           
