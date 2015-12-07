/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package transaction;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import modelo.dto.DataTable;
import remote.Sitio;
import remote.util.InterfaceManager;
import remote.util.QueryManager;

/**
 *
 * @author jdosornio
 */
public class TransactionManager {
    
    public static boolean insertReplicado(String[] tablas, DataTable ... datos) {
        boolean ok = true;
        
        try {
            short result = QueryManager.broadInsert(tablas, datos);
            
            if(result == 0) {
                ok = false;
                rollback();
            }
            else {
                commit();
            }
            
        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return ok;
    }
    
    public static void commit() throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();
        
        //Obtener todas las interfaces de sitio
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.Interfaces.values()) {
            Runnable hacerCommit = new Runnable() {
                @Override
                public void run() {
                    try {
                        Sitio sitio = InterfaceManager.getInterface(
                                InterfaceManager.getInterfaceServicio(interfaceSitio));
                 
                        if (sitio != null) {
                            boolean ok = sitio.commit();
                        
                            System.out.println("Thread de commit a la interface: " +
                                interfaceSitio + ", resultado = " + ok);
                        }
                    } catch (RemoteException | NotBoundException ex) {
                        Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            };
            
            Thread hilo = new Thread(hacerCommit);
            hilo.start();
            hilosInsert.add(hilo);
        }
        
        for (Thread hilo : hilosInsert) {
            hilo.join();
        }
        
        System.out.println("fin de commit global");
    }
    
    public static void rollback() throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();
        
        //Obtener todas las interfaces de sitio
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.Interfaces.values()) {
            Runnable hacerRollback = new Runnable() {
                @Override
                public void run() {
                    try {
                        Sitio sitio = InterfaceManager.getInterface(
                                InterfaceManager.getInterfaceServicio(interfaceSitio));
                 
                        if (sitio != null) {
                            boolean ok = sitio.rollback();
                        
                            System.out.println("Thread de rollback a la interface: " +
                                interfaceSitio + ", resultado = " + ok);
                        }
                    } catch (RemoteException | NotBoundException ex) {
                        Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            };
            
            Thread hilo = new Thread(hacerRollback);
            hilo.start();
            hilosInsert.add(hilo);
        }
        
        for (Thread hilo : hilosInsert) {
            hilo.join();
        }
        
        System.out.println("fin de rollback global");
    }
}