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
import modelo.dao.EmpleadoDAO;
import modelo.dto.DataTable;
import remote.Sitio7Int;

/**
 *
 * @author jdosornio
 */
public class AccesoLocal extends UnicastRemoteObject implements Sitio7Int {

    
    public AccesoLocal() throws RemoteException {
        
    }

    @Override
    public DataTable getEmpleado(String numeroEmp) throws RemoteException {
        return new EmpleadoDAO().get(numeroEmp);
    }

    @Override
    public DataTable getEmpleadosByImplementacion(int idImplementacion) throws RemoteException {
        return new EmpleadoDAO().getByImplementacion(idImplementacion);
    }

    @Override
    public short insert(String[] tablas, DataTable... datos) throws RemoteException {
        short ok = 1;
        BaseDAO dao = new BaseDAO();
        //Insertar todas las tablas....
        for (int i = 0; i < tablas.length; i++) {
            boolean noError = dao.add(tablas[i], datos[i]);
            
            if (!noError) {
                ok = 0;
                break;
            }
        }
        
        System.out.println("Inserción de " + tablas.length + " tablas, resultado: " +
                ok);
        
        return ok;
    }

    @Override
    public short update(String tabla, DataTable datos, Map attrWhere) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public short delete(String tabla, Map attrWhere) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean commit() throws RemoteException {
        System.out.println("Commit!");
        boolean ok = ConnectionManager.commit();
        ConnectionManager.cerrar();
        
        return ok;
    }

    @Override
    public boolean rollback() throws RemoteException {
        System.out.println("Rollback!");
        boolean ok = ConnectionManager.rollback();
        ConnectionManager.cerrar();
        
        return ok;
    }

    @Override
    public short updateEventosByProveedor(int idProveedor, int[] idsEvento) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public short updateEmpleadosByImplementacion(int idImplementacion, int[] idsEmpleado) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}