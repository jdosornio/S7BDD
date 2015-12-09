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

    private static final String EMPLEADO = "empleado";
    private static final int LLAVES = 1;
    private static final int NOMBRES = 0;

    public static boolean insertReplicado(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Global transaction----------");
        try {
            short result = QueryManager.broadInsert(savePKs, tabla, datos);

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

    public static boolean insertEmpleado(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Empleado transaction---------- ");

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

            short result = LLAVES;
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

                result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                        fragmentos[NOMBRES]) != null ? (short) 1 : (short) 0;
                System.out.println("Sitio 1: " + result);
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                        fragmentos[LLAVES]) != null ? (short) 1 : (short) 0;
                System.out.println("Sitio 2: " + result);
            } else {

                Map<String, Object> mapa = new HashMap<>();
                mapa.put("id", datos.getInt("plantel_id"));
                BaseDAO dao = new BaseDAO();

                DataTable plantel = dao.get("plantel", null, null, mapa);

                //se verifica en su nodo si se encuentra el plantel al que se insertara
                // cambiar por sus nodos el nombre de la variable de sitio y la interface
                if (plantel != null && plantel.getRowCount() != NOMBRES) {

                    fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);
//                    //este es su nodo ya no lo inserten de nuevo
                    result = (dao.add(EMPLEADO, fragmentos[LLAVES], false) != null) ? (short) LLAVES : (short) NOMBRES;
                    System.out.println("Sitio Local: " + result);

                    result *= QueryManager.uniInsert(false, Interfaces.SITIO_4,
                            EMPLEADO, fragmentos[NOMBRES]) != null ? (short) 1 : (short) 0;
                    System.out.println("Sitio 4: " + result);

                } else {
//                    revisar en los demas nodos
//                     tienen que verificar en los demas nodos en un solo sitio si se encuentra el plantel
//                     aqui se verifica la zona 1
//                    busca en la zona 1 si se encuentra el platel
                    Sitio sitio2 = InterfaceManager.getInterface(InterfaceManager.
                            getInterfaceServicio(Interfaces.SITIO_2));
                    plantel = sitio2.get("plantel", null, null, mapa);

                    if (plantel != null && plantel.getRowCount() != NOMBRES) {

                        //aqui se encuentra
                        fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);

                        result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                                fragmentos[NOMBRES]) != null ? (short) 1 : (short) 0;
                        System.out.println("Sitio 1: " + result);
                        result *= sitio2.insert(false, EMPLEADO,
                                fragmentos[LLAVES]) != null ? (short) 1 : (short) 0;
                        System.out.println("Sitio 2: " + result);
                    } else {
//                        aqui se veririca la zona 3
                        Sitio sitio7 = InterfaceManager.getInterface(InterfaceManager.
                                getInterfaceServicio(Interfaces.SITIO_7));
                        plantel = sitio7.get(tabla, null, null, mapa);

                        if (plantel != null && plantel.getRowCount() != NOMBRES) {
                            fragmentos = datos.fragmentarVertical(fragDatos, fragLlaves);

                            result = QueryManager.uniInsert(false, Interfaces.SITIO_5, EMPLEADO,
                                    fragmentos[LLAVES]) != null ? (short) 1 : (short) 0;
                            System.out.println("Sitio 5: " + result);

                            result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, EMPLEADO,
                                    fragmentos[LLAVES]) != null ? (short) 1 : (short) 0;
                            System.out.println("Sitio 6: " + result);

                            result *= QueryManager.uniInsert(false, Interfaces.SITIO_7, EMPLEADO,
                                    fragmentos[NOMBRES]) != null ? (short) 1 : (short) 0;
                            System.out.println("Sitio 7: " + result);

                        }
                    }
                }
            }

            if (result == NOMBRES) {
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

        System.out.println("---------End Empleado transaction----------");
        return ok;
    }

    public static boolean insertPlantel(boolean savePKs, String tabla,
            DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Plantel transaction---------- ");

        short result = 0;
        datos.rewind();
        datos.next();

        DataTable tablaResult;
        try {
            //cambien su sitio por local insert el mio está en la zona 2 
            if (datos.getInt("zona_id") == 1) {
                System.out.println("Zona 1");
                tablaResult = QueryManager.uniInsert(true, Interfaces.SITIO_1, tabla, datos);
                result = tablaResult != null ? (short) 1 : (short) 0;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, tabla, tablaResult)
                        != null ? (short) 1 : (short) 0;
            } else if (datos.getInt("zona_id") == 2) {
                System.out.println("Zona 2");
                tablaResult = QueryManager.localInsert(true, tabla, datos);
                result = tablaResult != null ? (short) 1 : (short) 0;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_4, tabla, datos)
                        != null ? (short) 1 : (short) 0;
            } else if (datos.getInt("zona_id") == 3) {
                System.out.println("Zona 3");
                tablaResult = QueryManager.uniInsert(true, Interfaces.SITIO_5, tabla, datos);
                result = tablaResult != null ? (short) 1 : (short) 0;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, tabla, datos)
                        != null ? (short) 1 : (short) 0;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_7, tabla, datos)
                        != null ? (short) 1 : (short) 0;
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
        }

        System.out.println("---------End Plantel transaction----------");
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
}