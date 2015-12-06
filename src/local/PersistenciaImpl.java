/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import modelo.dao.BaseDAO;
import modelo.dao.ConnectionManager;
import modelo.dto.DataTable;
import persistencia.Persistencia;
/**
 *
 * @author jdosornio
 */
public class PersistenciaImpl extends UnicastRemoteObject implements Persistencia  {

    public PersistenciaImpl() throws RemoteException {
        
    }

    @Override
    public boolean insert(String[] tablas, DataTable... datos) throws RemoteException {
        boolean ok = true;
        //En caso de que sea cualquier tabla hacer insert replicado, pero por
        //ahora sólo en este nodo...
        BaseDAO dao = new BaseDAO();
        
        //Insertar todas las tablas....
        for (int i = 0; i < tablas.length; i++) {
            //En esta parte se puede tanto llamar al dao (local) como a un insert
            //de la interface de cualquier sitio (remoto)
            boolean noError = dao.add(tablas[i], datos[i]);
            
            if (!noError) {
                ok = false;
                break;
            }
        }
        
        if(ok) {
            //Si todo salió bien realizar commit. Esto no se debe realizar así,
            //se tiene que realizar commit localmente una vez que todos los nodos
            //participantes realizaron el insert correctamente, pero por ahora
            //podemos dejarlo así
            ConnectionManager.commit();
            ConnectionManager.cerrar();
        }
        
        System.out.println("Inserción de " + tablas.length + " tablas, resultado: " +
                ok);
        
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
    
}