/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import modelo.dto.DataTable;

/**
 *
 * @author jdosornio
 */
public interface Sitio extends Remote {
    
    public void insert(String tabla, DataTable datos) throws RemoteException;
    
    public void update(String tabla, DataTable datos) throws RemoteException;
    
    public void delete(String tabla, DataTable ids) throws RemoteException;
    
    public void commit() throws RemoteException;
    
    public void rollback() throws RemoteException;
}