/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import modelo.dao.BaseDAO;
import modelo.dto.DataTable;
import persistencia.Persistencia;
import transaction.TransactionManager;

/**
 *
 * @author jdosornio
 */
public class PersistenciaImpl extends UnicastRemoteObject implements Persistencia {

    public PersistenciaImpl() throws RemoteException {

    }

    @Override
    public boolean insert(String[] tablas, DataTable... datos) throws RemoteException {
        boolean ok;

        //Tablas y datatables
        List<String> tablasReplicadas = new ArrayList<>();
        List<DataTable> dtReplicados = new ArrayList<>();

        //Insertar todas las tablas....
        for (int i = 0, j = 0; i < tablas.length; i++) {
            if (!tablas[i].equalsIgnoreCase("empleado")
                    && !tablas[i].equalsIgnoreCase("plantel")
                    && !tablas[i].equalsIgnoreCase("implementacion_evento_empleado")) {

                tablasReplicadas.add(tablas[i]);
                dtReplicados.add(datos[i]);
            }
        }

        String[] tablasReplicadasArr = new String[tablasReplicadas.size()];
        DataTable[] dtReplicadosArr = new DataTable[dtReplicados.size()];

        ok = TransactionManager.insertReplicado(true,
                tablasReplicadas.toArray(tablasReplicadasArr),
                dtReplicados.toArray(dtReplicadosArr));

        //Mandar las tablas a insertar a todos los nodos
        System.out.println("Inserción de " + tablas.length + " tablas, resultado: "
                + ok);

        return ok;
    }

    @Override
    public boolean update(String tabla, DataTable datos, Map<String, ?> attrWhere) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean delete(String tabla, Map<String, ?> attrWhere) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable get(String tabla, String[] columnas, String[] aliases, 
            Map<String, ?> attrWhere) throws RemoteException {

        DataTable dt = null;
        
        if (!tabla.equalsIgnoreCase("empleado")
                && !tabla.equalsIgnoreCase("plantel")
                && !tabla.equalsIgnoreCase("implementacion_evento_empleado")) {
            //Todas son consultas locales....
            dt = new BaseDAO().get(tabla, columnas, aliases, attrWhere);
        }
        
        return dt;
    }

}
