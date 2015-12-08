/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package transaction;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import modelo.dao.BaseDAO;
import modelo.dto.DataTable;
import modelo.util.ConnectionManager;
import remote.Sitio;
import remote.util.InterfaceManager;
import remote.util.InterfaceManager.Interfaces;
import remote.util.QueryManager;

/**
 *
 * @author jdosornio
 */
public class TransactionManager {
    
    public static boolean insertReplicado(boolean savePKs, String[] tablas,
            DataTable ... datos) {
        boolean ok = true;
        
        System.out.println("---------Start Global transaction----------");
        try {
            short result = QueryManager.broadInsert(savePKs, tablas, datos);
            
            if(result == 0) {
                ok = false;
                rollback();
            }
            else {
                commit();
            }
            
        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = false;
        }
        
        System.out.println("---------End Global transaction----------");
        return ok;
    }
    
    public static boolean insertEmpleado(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Global transaction---------- empleado");

        try {
            String[] fragDatos = {
                "numero",
                "primer_nombre",
                "segundo_nombre",
                "apellido_paterno",
                "apellido_materno",
                "puesto_id"
            };
            String[] fragLlaves = {
                "numero",
                "correo",
                "adscripcion_id",
                "departamento_id",
                "plantel_id",
                "direccion_id"
            };

            short result = 1;
            DataTable[] fragmentos;
            datos.rewind();
            datos.next();
            //si el id de adscripcion es != 2 entonces sera el insert siempre 
            // a la zona 1 y 2
            if (datos.getInt("adscripcion_id") != 2) {
                //Insert en sitio 1 y 2
                //fragmento datos fragmentos[0] aqui se encutra el nombre completo
                //fragento llaves fragmentos[1] aqui están la mayoría de las llaves
                fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);
                Sitio sitio1 = InterfaceManager.getInterface(InterfaceManager.
                        getInterfaceServicio(Interfaces.SITIO_1));

                Sitio sitio2 = InterfaceManager.getInterface(InterfaceManager.
                        getInterfaceServicio(Interfaces.SITIO_2));

                result = sitio1.insert(false, new String[]{"empleado"},
                        new DataTable[]{fragmentos[0]});

                result *= sitio2.insert(false, new String[]{"empleado"},
                        new DataTable[]{fragmentos[1]});

            } else {

                Map<String, Object> mapa = new HashMap<>();
                mapa.put("id", datos.getInt("plantel_id"));
                BaseDAO dao = new BaseDAO();

                DataTable plantel = dao.get("plantel", null, mapa);

                //se verifica en su nodo si se encuentra el plantel al que se insertara
                // cambiar por sus nodos el nombre de la variable de sitio y la interface
                if (plantel != null && plantel.getRowCount() != 0) {

                    fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);
                    //este es su nodo ya no lo inserten de nuevo
                    result = (dao.add("empleado", fragmentos[1], false)) ? (short) 1 : (short) 2;

                    Sitio sitio4 = InterfaceManager.getInterface(InterfaceManager.
                            getInterfaceServicio(Interfaces.SITIO_4));

                    result *= sitio4.insert(false, new String[]{"empleado"},
                            new DataTable[]{fragmentos[0]});
                } else {
                    //revisar en los demas nodos
                    // tienen que verificar en los demas nodos en un solo sitio si se encuentra el plantel
                    // aqui se verifica la zona 1
                    //busca en la zona 1 si se encuentra el platel
                    Sitio sitio2 = InterfaceManager.getInterface(InterfaceManager.
                            getInterfaceServicio(Interfaces.SITIO_2));

                    if (sitio2.get("plantel", null, mapa) != null && 
                            sitio2.get("plantel", null, mapa).getRowCount() != 0) {

                        //aqui se encuentra
                        fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);
                        Sitio sitio1 = InterfaceManager.getInterface(InterfaceManager.
                                getInterfaceServicio(Interfaces.SITIO_1));

                        result = sitio1.insert(false, new String[]{"empleado"},
                                new DataTable[]{fragmentos[0]});

                        result *= sitio2.insert(false, new String[]{"empleado"},
                                new DataTable[]{fragmentos[1]});

                    } else {
                        //aqui se veririca la zona 3
                        Sitio sitio7 = InterfaceManager.getInterface(InterfaceManager.
                                getInterfaceServicio(Interfaces.SITIO_7));
                        if (sitio7.get("plantel", null, mapa) != null &&
                                sitio7.get("plantel", null, mapa).getRowCount() != 0) {

                            //aqui se encuentra
                            fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);

                            Sitio sitio5 = InterfaceManager.getInterface(InterfaceManager.
                                    getInterfaceServicio(Interfaces.SITIO_5));
                            Sitio sitio6 = InterfaceManager.getInterface(InterfaceManager.
                                    getInterfaceServicio(Interfaces.SITIO_6));

                            result = sitio7.insert(false, new String[]{"empleado"},
                                    new DataTable[]{fragmentos[0]});

                            result *= sitio6.insert(false, new String[]{"empleado"},
                                    new DataTable[]{fragmentos[1]});

                            result *= sitio5.insert(false, new String[]{"empleado"},
                                    new DataTable[]{fragmentos[1]});

                        }
                    }
                }
            }
            
            

            if (result == 0) {
                ok = false;
                rollback();
            } else {
                commit();
            }

        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = false;
        } catch (RemoteException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NotBoundException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("---------End Global transaction----------");
        return ok;
    }
    
    public static void commit() throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();
        
        //Commit local
        ConnectionManager.commit();
        ConnectionManager.cerrar();
        
        //Obtener todas las interfaces de sitio
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.Interfaces.values()) {
            
            if(interfaceSitio.equals(Interfaces.LOCALHOST)) {
                continue;
            }
            
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
        
        //Rollback local
        ConnectionManager.rollback();
        ConnectionManager.cerrar();
        
        //Obtener todas las interfaces de sitio
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.Interfaces.values()) {
            
            if(interfaceSitio.equals(Interfaces.LOCALHOST)) {
                continue;
            }
            
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