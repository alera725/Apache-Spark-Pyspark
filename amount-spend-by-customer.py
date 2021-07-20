from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName('AmountSpent')
sc = SparkContext(conf = conf)

def parseLine(data):
	fields = data.split(',')
	clientId = int(fields[0])
	amount = float(fields[2])
	return(clientId, amount)

data = sc.textFile('/Users/alera/Documents/ARG/CURSOS/BIG DATA SPARK AND PYTHON/customer-orders.csv')
rdd = data.map(parseLine)

#Sort by amount spent 
totalByClient = rdd.reduceByKey(lambda x,y: round(x+y,2))

totalByClient_sort = totalByClient.map(lambda x: (x[1],x[0])).sortByKey() #Aqui cambiamos la key por el valor, ahora el valor es la key y no el ID para poder ordenarlos por valor (spent amount)

#not sorted
#totalByClient = rdd.reduceByKey(lambda x,y: round(x+y,2)) #Agrupamos cada cliente con la suma de todas sus cuentas 

results = totalByClient_sort.collect() #Eliminar el sort si se usa not sorted

for result in results:
	print(result)
