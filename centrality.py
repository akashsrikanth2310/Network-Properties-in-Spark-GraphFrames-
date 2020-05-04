from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from pyspark.sql.functions import explode
from pyspark.sql import Row

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
	
	# Get list of vertices. We'll generate all the shortest paths at
	# once using this list.
	# YOUR CODE HERE
    vertices = g.vertices
    verticesList = []
    for line in vertices.collect():
        verticesList.append(line.id)                   # verticesList holds all the vertices in a list
       
    paths = g.shortestPaths(landmarks = verticesList)  # finds the shortestPaths
	# first get all the path lengths.
	# Break up the map and group by ID for summing
    selected = paths.select('id', explode('distances')) #https://sparkbyexamples.com/spark/explode-spark-array-and-map-dataframe-column/
	# Sum by ID   
    requiredVals = selected.map(lambda line: (line[0],float(line[2])))   
    selectedVal = []
    groupandSum = requiredVals.reduceByKey(lambda line1,line2: line1 + line2)      #reducingByKey
    for line in groupandSum.collect():                                             #storing as a Row values
        value = Row(line[0],1/line[1])
        selectedVal.append(value)
	# Get the inverses and generate desired dataframe.
    return sqlContext.createDataFrame(selectedVal, ['id', 'closeness'])            #creatingDataFrame


    print("Reading in graph for problem 2.")
graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
	
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print("Calculating closeness.")
dataframe = closeness(g).sort('closeness',ascending=False)
dataframe.show()
dataframe.toPandas().to_csv("centrality_out.csv")

