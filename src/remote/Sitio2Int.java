/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote;

import java.rmi.RemoteException;
import modelo.dto.DataTable;

/**
 *
 * @author jdosornio
 */
public interface Sitio2Int extends Sitio {
    
    public DataTable getTemas(int idCurso) throws RemoteException;
}