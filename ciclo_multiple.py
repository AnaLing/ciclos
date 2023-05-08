from pyspark import SparkConf, SparkContext

# Configuración de Spark
conf = SparkConf().setAppName("Calculo de triciclos")
sc = SparkContext(conf=conf)

# Carpeta con los ficheros de entrada
input_folder = "carpeta_con_ficheros_de_entrada"

# Función para procesar las aristas y obtener los 3-ciclos
def process_edges(file_contents):
    edges = file_contents.split("\n")
    edges = [tuple(e.split(" ")) for e in edges if e]
    triciclos = []
    for e1 in edges:
        for e2 in edges:
            if e1 != e2 and e1[1] == e2[0]:
                e3 = (e1[0], e2[1])
                if e3 in edges:
                    triciclos.append((e1[0], e1[1], e2[1]))
    return triciclos

# Carga de los ficheros y procesamiento de las aristas
rdd = sc.wholeTextFiles(input_folder)
triciclos = rdd.flatMap(lambda x: process_edges(x[1])) \
               .distinct() \
               .sortBy(lambda x: x)

# Impresión de los triciclos encontrados
print("Triciclos encontrados:")
for t in triciclos.collect():
    print(t)

# Cierre de Spark
sc.stop()