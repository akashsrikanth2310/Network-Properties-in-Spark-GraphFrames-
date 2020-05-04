import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy
from pyspark.sql import Row

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def vertexFiltered(verticesargs,line):
    verticesflitered = verticesargs.filter('id != "' + line + '"')
    return verticesflitered

def edgesFiltered(edgesargs,line):
    edgesfiltered1 = edgesargs.filter('src != "' + line +'"')
    edgesfiltered2=  edgesfiltered1.filter('dst != "' + line +'"')
    return edgesfiltered2

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	# YOUR CODE HERE
    startingConenctedComponenets = g.connectedComponents().select('component').distinct()  #selecting distinct connected component
    startingConenctedComponenetsCount = startingConenctedComponenets.count()               #getting count of components
    finalVal = []
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
    if usegraphframe:
		# Get vertex list for serial iteration
		# YOUR CODE HERE
        vertices = g.vertices
        verticesList = []
        for line in vertices.collect():              #creating vertex list
            verticesList.append(line.id)                                                
        # For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		# YOUR CODE HERE
        vertices = g.vertices     #getting vertices
        edges = g.edges           #getting edges
        for line in verticesList:
            verticesflitered=vertexFiltered(vertices,line)       #filteredvertices
            edgesfiltered=edgesFiltered(edges,line)              #filterededges
            graphFrameCreated = GraphFrame(verticesflitered, edgesfiltered) #creating graphframe
            nowConnectedComponent = graphFrameCreated.connectedComponents().select('component').distinct()
            nowConnectedComponentCount = nowConnectedComponent.count()  #getting count
            finalVal = []
            if(nowConnectedComponentCount > startingConenctedComponenetsCount):
                finalVal.append(Row(line,1))
            else:
                finalVal.append(Row(line,0))
            return sqlContext.createDataFrame(finalVal,['id','articulation'])
        

		
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
    else:
        # YOUR CODE HERE
        graphx = nx.Graph()
        vertices = g.vertices
        verticesList = []
        for line in vertices.collect():
            verticesList.append(line.id)
        edges = g.edges
        edgesList = []
        for line in g.edges.collect():
            key = line.src
            val = line.dst
            edgesList.append((key,val))
        graphx.add_nodes_from(verticesList)      #adding vertices
        graphx.add_edges_from(edgesList)         #adding edges
        def connectedComponentsCount(c):
            graph = deepcopy(graphx)
            graph.remove_node(c)
            return nx.number_connected_components(graph)
        lineval = []
        for line in g.vertices.collect():
            if(connectedComponentsCount(line.id) > startingConenctedComponenetsCount):
                lineval.append(Row(line.id,1))
            else:
                lineval.append(Row(line.id,0))
        return sqlContext.createDataFrame(lineval, ['id', 'articulation'])
        
		

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
df.toPandas().to_csv("articulation_out.csv")

#Runtime for below is more than 2 hours
#print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
#init = time.time()
#df = articulations(g, True)
#print("Execution time: %s seconds" % (time.time() - init))
#print("Articulation points:")
#df.filter('articulation = 1').show(truncate=False)
