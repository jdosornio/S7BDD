/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package persistencia;

import java.rmi.Remote;
import java.rmi.RemoteException;
import modelo.dto.DataTable;

/**
 *
 * @author jdosornio
 */
public interface Persistencia extends Remote {
    
    public DataTable getUsuario(String usuario) throws RemoteException;
    
    public DataTable getTemas(int idCurso) throws RemoteException;
    
    public DataTable getReactivosCreados(String usuario, int idCurso) throws RemoteException;
    
}