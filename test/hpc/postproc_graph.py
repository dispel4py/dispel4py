from verce.workflow_graph import WorkflowGraph
from test.hpc.postproc import ReadJSON, WatchDirectory, Specfem3d2Stream, WavePlot_INGV, StreamToSeedFile
from verce.seismo import seismo
import os


graph = WorkflowGraph()
read = ReadJSON()
watcher = WatchDirectory()
waveplot = WavePlot_INGV()
specfem2stream = Specfem3d2Stream()
seedtostream=StreamToSeedFile()

specfem2stream.stationsFile=os.environ['RUN_PATH']+'/stations'
specfem2stream.controlParameters = { 'runId' : '12345', 'username' : 'amyrosa',  'outputdest' : "./" }

waveplot.appParameters = { 'filedestination' : '/OUTPUT_FILES/TRANSFORMED/PLOT/' }
waveplot.controlParameters = { 'runId' : '12345', 'username' : 'amyrosa', 'outputdest' : os.environ['EVENT_PATH'] }

seedtostream.appParameters = { 'filedestination' : '/OUTPUT_FILES/TRANSFORMED/SEED/' }
seedtostream.controlParameters = { 'runId' : '12345', 'username' : 'amyrosa',  'outputdest' : os.environ['EVENT_PATH'] }


graph.connect(read, ReadJSON.OUTPUT_NAME, watcher, WatchDirectory.INPUT_NAME)
graph.connect(watcher, WatchDirectory.OUTPUT_NAME, specfem2stream, seismo.INPUT_NAME)
graph.connect(specfem2stream, seismo.OUTPUT_DATA, waveplot, seismo.INPUT_NAME)
graph.connect(specfem2stream, seismo.OUTPUT_DATA, seedtostream, seismo.INPUT_NAME)
