import csv
import os
class TransformCSV():
    def __init__(self, nombre) :
        self.archivo_csv ='Python/data/'+nombre

        self.datos = []

        with open(self.archivo_csv, newline='', encoding='utf-8') as csvfile:
            lector_csv = csv.reader(csvfile)
            for fila in lector_csv:
                
                self.datos.append(fila)
        