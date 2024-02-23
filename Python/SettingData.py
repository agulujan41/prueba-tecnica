import DBConexion
import TransformCSV as TransformCSV
class SettingData():
    def __init__(self) :
        self.db =DBConexion.DBConexion()
        #crearemos ambas tablas
        self.db.run_query("""
            CREATE TABLE IF NOT EXISTS CLAP (
                INICIO6_TARJETA INT(7),
                FINAL4_TARJETA INT(5),
                TIPO_TRX TEXT,
                MONTO DOUBLE,
                FECHA_TRANSACCION DATETIME,
                CODIGO_AUTORIZACION TEXT,
                ID_BANCO TEXT,
                FECHA_RECEPCION_BANCO DATE
            )

        """) 
        self.db.run_query("""
            CREATE TABLE IF NOT EXISTS BANSUR  (
                TARJETA BIGINT(20),
                TIPO_TRX TEXT,
                MONTO DOUBLE,
                FECHA_TRANSACCION DATE,
                CODIGO_AUTORIZACION TEXT,
                ID_ADQUIRIENTE TEXT,
                FECHA_RECEPCION DATE
             )
            """)
        
        self.__initData()
        self.__completeData()
        self.db.close()
    def __initData(self):
        cantT1 = self.db.run_query('SELECT COUNT(*) FROM  ptsql.bansur')
        cantT2 = self.db.run_query('SELECT COUNT(*) FROM ptsql.clap')
        if cantT1[0][0] == 0 :  
            csvBANSUR = TransformCSV.TransformCSV('BANSUR.csv')
            for fila in range (1,len(csvBANSUR.datos)):
                csvBANSUR.datos[fila][3] =  csvBANSUR.datos[fila][3][0:4] +"-" + csvBANSUR.datos[fila][3][4:6] + "-" + csvBANSUR.datos[fila][3][6:8]
                self.db.run_query(f"""INSERT INTO `bansur` 
                                  (`TARJETA`, `TIPO_TRX`, `MONTO`, `FECHA_TRANSACCION`, `CODIGO_AUTORIZACION`, `ID_ADQUIRIENTE`, `FECHA_RECEPCION`) 
                                  VALUES ('{ csvBANSUR.datos[fila][0]}', 
                                  '{ csvBANSUR.datos[fila][1]}', 
                                  '{csvBANSUR.datos[fila][2]}', 
                                  '{csvBANSUR.datos[fila][3]}', 
                                  '{csvBANSUR.datos[fila][4]}', 
                                  '{csvBANSUR.datos[fila][5]}', 
                                  '{csvBANSUR.datos[fila][6]}')""")
          
        if  cantT2[0][0]== 0:  
                 
                 
            csvCLAP = TransformCSV.TransformCSV('CLAP.csv')
            for fila in range (1,len(csvCLAP.datos)):
                self.db.run_query(f"""INSERT INTO `clap` 
                                  (`INICIO6_TARJETA`, `FINAL4_TARJETA`, `TIPO_TRX`, `MONTO`, `FECHA_TRANSACCION`, `CODIGO_AUTORIZACION`, `ID_BANCO`, `FECHA_RECEPCION_BANCO`) 
                                  VALUES (
                                  '{csvCLAP.datos[fila][0]}',
                                  '{csvCLAP.datos[fila][1]}', 
                                  '{csvCLAP.datos[fila][2]}', 
                                  '{csvCLAP.datos[fila][3]}', 
                                  '{csvCLAP.datos[fila][4]}', 
                                  '{csvCLAP.datos[fila][5]}',
                                  '{csvCLAP.datos[fila][6]}', 
                                  '{csvCLAP.datos[fila][7]}')""")
                
    def __completeData(self):
        cantT1 = self.db.run_query('SELECT COUNT(*) FROM  ptsql.bansur')
        cantT2 = self.db.run_query('SELECT COUNT(*) FROM ptsql.clap')
      
        csvBANSUR = TransformCSV.TransformCSV('BANSUR.csv')
        for fila in range (cantT1[0][0],len(csvBANSUR.datos)):
            csvBANSUR.datos[fila][3] =  csvBANSUR.datos[fila][3][0:4] +"-" + csvBANSUR.datos[fila][3][4:6] + "-" + csvBANSUR.datos[fila][3][6:8]
            self.db.run_query(f"""INSERT INTO `bansur` 
                                (`TARJETA`, `TIPO_TRX`, `MONTO`, `FECHA_TRANSACCION`, `CODIGO_AUTORIZACION`, `ID_ADQUIRIENTE`, `FECHA_RECEPCION`) 
                                VALUES ('{ csvBANSUR.datos[fila][0]}', 
                                '{ csvBANSUR.datos[fila][1]}', 
                                '{csvBANSUR.datos[fila][2]}', 
                                '{csvBANSUR.datos[fila][3]}', 
                                '{csvBANSUR.datos[fila][4]}', 
                                '{csvBANSUR.datos[fila][5]}', 
                                '{csvBANSUR.datos[fila][6]}')""")
          
                 
            
        csvCLAP = TransformCSV.TransformCSV('CLAP.csv')
        for fila in range (cantT2[0][0],len(csvCLAP.datos)):
            self.db.run_query(f"""INSERT INTO `clap` 
                                (`INICIO6_TARJETA`, `FINAL4_TARJETA`, `TIPO_TRX`, `MONTO`, `FECHA_TRANSACCION`, `CODIGO_AUTORIZACION`, `ID_BANCO`, `FECHA_RECEPCION_BANCO`) 
                                VALUES (
                                '{csvCLAP.datos[fila][0]}',
                                '{csvCLAP.datos[fila][1]}', 
                                '{csvCLAP.datos[fila][2]}', 
                                '{csvCLAP.datos[fila][3]}', 
                                '{csvCLAP.datos[fila][4]}', 
                                '{csvCLAP.datos[fila][5]}',
                                '{csvCLAP.datos[fila][6]}', 
                                '{csvCLAP.datos[fila][7]}')""")
sd = SettingData()
