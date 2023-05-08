from pyspark import SparkContext

def read_edges(file):
    #Lee un fichero y devuelve un RDD de aristas.
    return sc.textFile(file).map(lambda line: tuple(line.split()))

def get_vertices(edges):
    #Obtiene los vértices que aparecen en un RDD de aristas.
    return edges.flatMap(lambda e: [e[0], e[1]]).distinct()

def find_local_triangles(edges):
    #Encuentra los 3-ciclos locales a un RDD de aristas
    vertices = get_vertices(edges)
    triangles = (edges
                 # Join con el transpuesto
                 .map(lambda e: (e[1], e[0]))
                 .join(edges)
                 # Filtrar triciclos
                 .filter(lambda x: x[1][0] != x[1][1])
                 .map(lambda x: tuple(sorted([x[0], x[1][0], x[1][1]])))
                 # Eliminar duplicados
                 .distinct())
    return triangles

# Crear el contexto de Spark
sc = SparkContext(appName="triangles")

# Leer los ficheros de entrada y calcular los triciclos locales
files = ["file1.txt", "file2.txt", "file3.txt"]
triangles_local = [find_local_triangles(read_edges(file)) for file in files]

# Unir los RDDs de triciclos locales y eliminar duplicados
triangles = (sc.union(triangles_local)
             .distinct())

# Imprimir los triciclos globales
print(triangles.collect())

# Detener el contexto de Spark
sc.stop()