from pyspark import SparkContext

# Inicializar SparkContext
sc = SparkContext("local", "3-ciclos")

# Cargar datos del grafo como una lista de aristas
datos = [("A", "B"), ("A", "C"), ("B", "C"), ("C", "D"), ("D", "E"), ("E", "F"), ("F", "D")]
rdd = sc.parallelize(datos)

# Generar todas las combinaciones posibles de 3 vértices
combinaciones = rdd.flatMap(lambda x: [(x[0], x[1], y) for y in (x[0], x[1])]) \
                  .distinct()

# Filtrar las combinaciones que forman un 3-ciclo
tres_ciclos = combinaciones.filter(lambda x: (x[0], x[1]) in datos and (x[0], x[2]) in datos and (x[1], x[2]) in datos)

# Retornar una lista con los 3-ciclos encontrados
print(tres_ciclos.collect())