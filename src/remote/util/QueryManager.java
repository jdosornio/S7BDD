/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote.util;

import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import modelo.dao.BaseDAO;
import modelo.dto.DataTable;
import remote.Sitio;
import remote.util.InterfaceManager.Interfaces;

/**
 *
 * @author jdosornio
 */
public class QueryManager {

    private static volatile short transactionOk;

    public static DataTable localInsert(boolean savePKs, String tabla,
            DataTable datos) {

        DataTable ok;
        BaseDAO dao = new BaseDAO();
        //Insertar todas las tablas....
        ok = dao.add(tabla, datos, savePKs);

        System.out.println("Inserción de " + tabla + " , resultado: "
                + ok);

        return ok;
    }
        
    /**
     * Inserta los datos de todas las tablas en la interface del sitio elegido.
     *
     * @param savePKs guardar las llaves primarias generadas
     * @param interfaceSitio la interface del sitio al que se desea insertar
     * @param tabla el arreglo de nombres de tablas donde se insertará
     * @param datos el arreglo de DataTables que se desean insertar en el orden
     * en el que están los nombres de las tablas en el arreglo
     *
     * @return 1 en caso de que todo ocurra normalmente, 0 en caso contrario.
     */
    public static DataTable uniInsert(boolean savePKs, Interfaces interfaceSitio,
            String tabla, DataTable datos) {
        DataTable ok = null;
        try {
            //obtener la interface
            Sitio sitio = InterfaceManager.getInterface(
                    InterfaceManager.getInterfaceServicio(interfaceSitio));

            //insertar los datos
            if (sitio != null) {
                ok = sitio.insert(savePKs, tabla, datos);

                System.out.println("Insert en el sitio: "
                        + interfaceSitio + ", resultado = " + ok);
            }

        } catch (ConnectException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        } catch (RemoteException | NotBoundException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        }

        return ok;
    }

    /**
     * Inserta los datos de todas las tablas en todos los sitios que están
     * registrados en este nodo.
     *
     * @param savePKs guardar las llaves primarias generadas
     * @param tablas el arreglo de nombres de tablas donde se insertará
     * @param datos el arreglo de DataTables que se desean insertar en el orden
     * en el que están los nombres de las tablas en el arreglo
     *
     * @return 1 en caso de que todo ocurra normalmente, 0 en caso contrario.
     * @throws InterruptedException en caso de que ocurra un error con los
     * threads
     */
    public static synchronized short broadInsert(boolean savePKs, String tabla,
            DataTable datos)
            throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();

        //TRANSACTION_OK.set((short)1);
        transactionOk = (localInsert(savePKs, tabla, datos) != null ? (short) 1 : (short) 0);

//        System.out.println("Thread principal solicitante: transacionOk = 1");
//        uniInsert(savePKs, Interfaces.LOCALHOST, tablas, datos);
        System.out.println("savePKs: " + savePKs + " Id: " + datos.getValueAt(0, 0));

        //Obtener todas las interfaces de sitio
        for (Interfaces interfaceSitio : InterfaceManager.getInterfacesRegistradas()) {

            if (interfaceSitio.equals(Interfaces.LOCALHOST)) {
                continue;
            }

            Runnable insertar = new Runnable() {
                @Override
                public void run() {
//                    short resultadoTodos = TRANSACTION_OK.get();
//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoTodos = " + resultadoTodos);
//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoTodos = " + transactionOk);

                    short resultadoActual = uniInsert(true, interfaceSitio, tabla, datos)
                            != null ? (short) 1 : (short) 0;

//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoActual = " + resultadoActual);
                    //short resultadoNuevo = (short)(resultadoTodos * resultadoActual);
                    //TRANSACTION_OK.set(resultadoNuevo);
                    transactionOk *= (short) resultadoActual;

//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoNuevo = " + transactionOk);
                }
            };

            Thread hilo = new Thread(insertar);
            hilo.start();
            hilosInsert.add(hilo);
        }

        for (Thread hilo : hilosInsert) {
            hilo.join();
        }

//        System.out.println("Thread principal solicitante: transactionOk = " + 
//                TRANSACTION_OK.get());
        System.out.println("Thread principal solicitante: transactionOk = "
                + transactionOk);

        return transactionOk;
    }
    
    public static Boolean localUpdate(String tabla, DataTable datos,
            Map<String, ?> attrWhere) {
        
        Boolean ok;
        //Actualizar tabla....
        ok = new BaseDAO().update(tabla, datos, attrWhere);

        System.out.println("Actualización de " + tabla + " , resultado: "
                + ok);

        return (ok == true) ? ok : null;
    }
    
    public static Boolean uniUpdate(Interfaces interfaceSitio, String tabla,
            DataTable datos, Map<String, ?> attrWhere) {
        Boolean ok = null;
        try {
            //obtener la interface
            Sitio sitio = InterfaceManager.getInterface(
                    InterfaceManager.getInterfaceServicio(interfaceSitio));

            //insertar los datos
            if (sitio != null) {
                ok = sitio.update(tabla, datos, attrWhere);

                System.out.println("Update en el sitio: "
                        + interfaceSitio + ", resultado = " + ok);
            }

        } catch (ConnectException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        } catch (RemoteException | NotBoundException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        }

        return (ok == true) ? ok : null;
    }
    
    public static synchronized short broadUpdate(String tabla, DataTable datos,
            Map<String, ?> attrWhere)
            throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();

        //TRANSACTION_OK.set((short)1);
        transactionOk = (localUpdate(tabla, datos, attrWhere) != null ? (short) 1 : (short) 0);

        //Obtener todas las interfaces de sitio
        for (Interfaces interfaceSitio : InterfaceManager.getInterfacesRegistradas()) {

            if (interfaceSitio.equals(Interfaces.LOCALHOST)) {
                continue;
            }

            Runnable actualizar = new Runnable() {
                @Override
                public void run() {

                    short resultadoActual = uniUpdate(interfaceSitio, tabla,
                            datos, attrWhere) != null ? (short) 1 : (short) 0;

                    transactionOk *= (short) resultadoActual;

                }
            };

            Thread hilo = new Thread(actualizar);
            hilo.start();
            hilosInsert.add(hilo);
        }

        for (Thread hilo : hilosInsert) {
            hilo.join();
        }

        System.out.println("Thread principal solicitante: transactionOk = "
                + transactionOk);

        return transactionOk;
    }
   
    public static Boolean localDelete(String tabla, Map<String, ?> attrWhere) {
        Boolean ok;
        //Eliminar tabla....
        ok = new BaseDAO().delete(tabla, attrWhere);

        System.out.println("Eliminación de " + tabla + " , resultado: "
                + ok);

        return (ok == true) ? ok : null;
    }
    
    public static Boolean uniDelete(Interfaces interfaceSitio, String tabla,
            Map<String, ?> attrWhere) {
        Boolean ok = null;
        try {
            //obtener la interface
            Sitio sitio = InterfaceManager.getInterface(
                    InterfaceManager.getInterfaceServicio(interfaceSitio));

            //insertar los datos
            if (sitio != null) {
                ok = sitio.delete(tabla, attrWhere);

                System.out.println("Delete en el sitio: "
                        + interfaceSitio + ", resultado = " + ok);
            }

        } catch (ConnectException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        } catch (RemoteException | NotBoundException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        }

        return (ok == true) ? ok : null;
    }
    
    public static synchronized short broadDelete(String tabla, Map<String, ?> attrWhere)
            throws InterruptedException {
        List<Thread> hilosInsert = new ArrayList<>();

        //TRANSACTION_OK.set((short)1);
        transactionOk = (localDelete(tabla, attrWhere) != null ? (short) 1 : (short) 0);

//        System.out.println("Thread principal solicitante: transacionOk = 1");
//        uniInsert(savePKs, Interfaces.LOCALHOST, tablas, datos);
//        System.out.println("savePKs: " + savePKs + " Id: " + datos.getValueAt(0, 0));
        //Obtener todas las interfaces de sitio
        for (Interfaces interfaceSitio : InterfaceManager.getInterfacesRegistradas()) {

            if (interfaceSitio.equals(Interfaces.LOCALHOST)) {
                continue;
            }

            Runnable borrar = new Runnable() {
                @Override
                public void run() {
//                    short resultadoTodos = TRANSACTION_OK.get();
//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoTodos = " + resultadoTodos);
//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoTodos = " + transactionOk);

                    short resultadoActual = uniDelete(interfaceSitio, tabla, attrWhere)
                            != null ? (short) 1 : (short) 0;

//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoActual = " + resultadoActual);
                    //short resultadoNuevo = (short)(resultadoTodos * resultadoActual);
                    //TRANSACTION_OK.set(resultadoNuevo);
                    transactionOk *= (short) resultadoActual;

//                    System.out.println("Thread de inserción a la interface: " + 
//                            interfaceSitio + ", resultadoNuevo = " + transactionOk);
                }
            };

            Thread hilo = new Thread(borrar);
            hilo.start();
            hilosInsert.add(hilo);
        }

        for (Thread hilo : hilosInsert) {
            hilo.join();
        }

//        System.out.println("Thread principal solicitante: transactionOk = " + 
//                TRANSACTION_OK.get());
        System.out.println("Thread principal solicitante: transactionOk = "
                + transactionOk);

        return transactionOk;
    }
    
    public static DataTable uniGet(Interfaces interfaceSitio, String tableName,
            String[] projectColumns, String[] projectAliases, Map<String, ?> attrWhere) {
        DataTable ok = null;
        try {
            if (interfaceSitio == Interfaces.LOCALHOST) {
                ok = new BaseDAO().get(tableName, projectColumns, projectAliases, attrWhere);
                System.out.println("Insert en el sitio: "
                        + interfaceSitio + ", resultado = " + ok);
            } else {
                Sitio sitio = InterfaceManager.getInterface(
                        InterfaceManager.getInterfaceServicio(interfaceSitio));

                //insertar los datos
                if (sitio != null) {

                    ok = sitio.get(tableName, projectColumns, projectAliases, attrWhere);

                    System.out.println("Insert en el sitio: "
                            + interfaceSitio + ", resultado = " + ok);
                }
            }

        } catch (RemoteException | NotBoundException ex) {
            Logger.getLogger(QueryManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = null;
        }
        return ok;
    }

}