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
import modelo.util.ConnectionManager;
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
        
        //Insertar todas las tablas....
        for (int i = 0; i < tablas.length; i++) {
            
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