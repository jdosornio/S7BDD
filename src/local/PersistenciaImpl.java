/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
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
    public DataTable getUsuario(String usuario) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable getTemas(int idCurso) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable getReactivosCreados(String usuario, int idCurso) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}