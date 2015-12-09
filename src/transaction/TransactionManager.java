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

    private static final String EMPLEADO = "empleado";
    private static final String PLANTEL = "plantel";
    private static final short BIEN = 1;
    private static final int LLAVES = BIEN;
    private static final short MAL = 0;
    private static final int NOMBRES = MAL;

    public static boolean insertReplicado(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Global transaction----------");
        try {
            short result = QueryManager.broadInsert(savePKs, tabla, datos);

            if (result == MAL) {
                ok = false;
                rollback();
            } else {
                commit();
            }

        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = false;
        }

        System.out.println("---------End Global transaction----------");
        return ok;
    }

    public static boolean insertEmpleado(DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Empleado transaction---------- ");

        String[] fragDatos = {"numero", "primer_nombre", "segundo_nombre",
            "apellido_paterno", "apellido_materno", "puesto_id"};
        String[] fragLlaves = {"numero", "correo", "adscripcion_id",
            "departamento_id", "plantel_id", "direccion_id"};
        short result = MAL;
        DataTable[] fragmentos;
        datos.rewind();
        datos.next();
        List<Interfaces> inter = new ArrayList<>();
        fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);
        if (datos.getInt("adscripcion_id") != 2) {
            //Insert en sitio 1 y 2

            result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                    fragmentos[NOMBRES]) != null ? BIEN : MAL;
            System.out.println("Sitio 1: " + result);
            result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                    fragmentos[LLAVES]) != null ? BIEN : MAL;
            System.out.println("Sitio 2: " + result);

            inter.add(Interfaces.SITIO_1);
            inter.add(Interfaces.SITIO_2);
        } else {

            Map<String, Object> condicion = new HashMap<>();
            condicion.put("id", datos.getInt("plantel_id"));

            DataTable plantel = QueryManager.uniGet(Interfaces.LOCALHOST,
                    PLANTEL, null, null, condicion);

            //se verifica en su nodo si se encuentra el plantel al que se insertara
            // cambiar por sus nodos el nombre de la variable de sitio y la interface
            if (plantel != null && plantel.getRowCount() != 0) {
                //este es su nodo ya no lo inserten de nuevo

                result = QueryManager.localInsert(false, EMPLEADO, fragmentos[LLAVES])
                        != null ? BIEN : MAL;

                System.out.println("Sitio Local: " + result);

                result *= QueryManager.uniInsert(false, Interfaces.SITIO_4,
                        EMPLEADO, fragmentos[NOMBRES]) != null ? BIEN : MAL;
                System.out.println("Sitio 4: " + result);

                inter.add(Interfaces.LOCALHOST);
                inter.add(Interfaces.SITIO_4);

            } else {
//                    revisar en los demas nodos
//                     tienen que verificar en los demas nodos en un solo sitio si se encuentra el plantel
//                     aqui se verifica la zona 1
//                    busca en la zona 1 si se encuentra el platel

                plantel = QueryManager.uniGet(Interfaces.SITIO_2, PLANTEL,
                        null, null, condicion);

                if (plantel != null && plantel.getRowCount() != 0) {
                    //aqui se encuentra

                    result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                            fragmentos[NOMBRES]) != null ? BIEN : MAL;
                    System.out.println("Sitio 1: " + result);

                    result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                            fragmentos[LLAVES]) != null ? BIEN : MAL;

                    System.out.println("Sitio 2: " + result);

                    inter.add(Interfaces.SITIO_1);
                    inter.add(Interfaces.SITIO_2);

                } else {
//                        aqui se veririca la zona 3

                    plantel = QueryManager.uniGet(Interfaces.SITIO_7, PLANTEL,
                            null, null, condicion);

                    if (plantel != null && plantel.getRowCount() != 0) {

                        result = QueryManager.uniInsert(false, Interfaces.SITIO_5, EMPLEADO,
                                fragmentos[LLAVES]) != null ? BIEN : MAL;
                        System.out.println("Sitio 5: " + result);

                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, EMPLEADO,
                                fragmentos[LLAVES]) != null ? BIEN : MAL;
                        System.out.println("Sitio 6: " + result);

                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_7, EMPLEADO,
                                fragmentos[NOMBRES]) != null ? BIEN : MAL;
                        System.out.println("Sitio 7: " + result);

                        inter.add(Interfaces.SITIO_5);
                        inter.add(Interfaces.SITIO_6);
                        inter.add(Interfaces.SITIO_7);

                    }
                }
            }
        }
        if (result == BIEN) {
            commit(inter);
        } else {
            rollback(inter);
        }

        System.out.println("---------End Empleado transaction----------");
        return ok;
    }

    public static boolean insertPlantel(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Plantel transaction---------- ");

        short result = MAL;
        datos.rewind();
        datos.next();
        DataTable tablaResult;
        List<Interfaces> inter = new ArrayList<>();

        if (datos.getInt("zona_id") == 1) {

            System.out.println("Zona 1");
            tablaResult = QueryManager.uniInsert(true, Interfaces.SITIO_2, tabla, datos);
            result = tablaResult != null ? BIEN : MAL;
            result *= QueryManager.uniInsert(false, Interfaces.SITIO_1, tabla, tablaResult)
                    != null ? BIEN : MAL;

            inter.add(Interfaces.SITIO_1);
            inter.add(Interfaces.SITIO_2);

        } else if (datos.getInt("zona_id") == 2) {

            System.out.println("Zona 2");
            tablaResult = QueryManager.localInsert(true, tabla, datos);
            result = tablaResult != null ? BIEN : MAL;
            result *= QueryManager.uniInsert(false, Interfaces.SITIO_4, tabla, tablaResult)
                    != null ? BIEN : MAL;

            inter.add(Interfaces.LOCALHOST);
            inter.add(Interfaces.SITIO_4);

        } else if (datos.getInt("zona_id") == 3) {

            System.out.println("Zona 3");
            tablaResult = QueryManager.uniInsert(true, Interfaces.SITIO_5, tabla, datos);
            result = tablaResult != null ? BIEN : MAL;
            result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, tabla, tablaResult)
                    != null ? BIEN : MAL;
            result *= QueryManager.uniInsert(false, Interfaces.SITIO_7, tabla, tablaResult)
                    != null ? BIEN : MAL;

            inter.add(Interfaces.SITIO_5);
            inter.add(Interfaces.SITIO_6);
            inter.add(Interfaces.SITIO_7);
        }

        if (result == MAL) {
            ok = false;
            rollback(inter);
        } else {
            commit(inter);
        }

        System.out.println("---------End Plantel transaction----------");
        return ok;
    }
    
    public static boolean updateReplicado(String tabla, DataTable datos,
            Map<String, ?> attrWhere) {
        
        boolean ok = true;

        System.out.println("---------Start Global transaction----------");
        try {
            short result = QueryManager.broadUpdate(tabla, datos, attrWhere);

            if (result == 0) {
                ok = false;
                rollback();
            } else {
                commit();
            }

        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = false;
        }

        System.out.println("---------End Global transaction----------");
        return ok;
    }
    
    public static boolean deleteReplicado(String tabla, Map<String, ?> attrWhere) {
        boolean ok = true;

        System.out.println("---------Start Global transaction----------");
        try {
            short result = QueryManager.broadDelete(tabla, attrWhere);

            if (result == 0) {
                ok = false;
                rollback();
            } else {
                commit();
            }

        } catch (InterruptedException ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
            ok = false;
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
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.getInterfacesRegistradas()) {

            if (interfaceSitio.equals(Interfaces.LOCALHOST)) {
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

                            System.out.println("Thread de commit a la interface: "
                                    + interfaceSitio + ", resultado = " + ok);
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
        for (InterfaceManager.Interfaces interfaceSitio : InterfaceManager.getInterfacesRegistradas()) {

            if (interfaceSitio.equals(Interfaces.LOCALHOST)) {
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

                            System.out.println("Thread de rollback a la interface: "
                                    + interfaceSitio + ", resultado = " + ok);
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

    public static void commit(List<Interfaces> interfaces) {

        for (Interfaces interfaceSitio : interfaces) {
            if (interfaceSitio == Interfaces.LOCALHOST) {
                ConnectionManager.commit();
                ConnectionManager.cerrar();
            } else {
                try {
                    Sitio sitio = InterfaceManager.getInterface(
                            InterfaceManager.getInterfaceServicio(interfaceSitio));
                    if (sitio != null) {
                        boolean ok = sitio.commit();

                        System.out.println("Thread de commit a la interface: "
                                + interfaceSitio + ", resultado = " + ok);
                    }
                } catch (RemoteException | NotBoundException ex) {
                    Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void rollback(List<Interfaces> interfaces) {
        for (Interfaces interfaceSitio : interfaces) {
            if (interfaceSitio == Interfaces.LOCALHOST) {
                ConnectionManager.rollback();
                ConnectionManager.cerrar();
            } else {
                try {
                    Sitio sitio = InterfaceManager.getInterface(
                            InterfaceManager.getInterfaceServicio(interfaceSitio));
                    if (sitio != null) {
                        boolean ok = sitio.rollback();

                        System.out.println("Thread de commit a la interface: "
                                + interfaceSitio + ", resultado = " + ok);
                    }
                } catch (RemoteException | NotBoundException ex) {
                    Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

}