/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package transaction;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
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

    private static final String EMPLEADO_ID = "numero";
    private static final String EMPLEADO_PLANTEL_ID = "plantel_id";
    private static final String EMPLEADO_CORREO = "correo";
    private static final String EMPLEADO_ADSCRIPCION_ID = "adscripcion_id";
    private static final String[] FRAG_LLAVES = {EMPLEADO_ID,
        EMPLEADO_CORREO, EMPLEADO_ADSCRIPCION_ID, "departamento_id", EMPLEADO_PLANTEL_ID, "direccion_id"};

    private static final String[] FRAG_DATOS = {EMPLEADO_ID, "primer_nombre", "segundo_nombre",
        "apellido_paterno", "apellido_materno", "puesto_id"};
    private static final String PLANTEL_ID = "id";
    private static final String EMPLEADO = "empleado";
    private static final String PLANTEL = "plantel";
    private static final String IMPLEMENTACION_EVENTO_EMPLEADO = "implementacion_evento_empleado";
    private static final String IMPLEMENTACION_EVENTO_ID = "implementacion_evento_id";
    private static final String EMPLEADO_NUMERO = "empleado_numero";

    private static final String PLANTEL_ZONA_ID = "zona_id";
    private static final short BIEN = 1;
    private static final int LLAVES = BIEN;
    private static final short MAL = 0;
    private static final int DATOS = MAL;

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

    //Modificar para su sitio
    public static boolean insertEmpleado(DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Insert Empleado transaction---------- ");
        datos.rewind();
        datos.next();

        Integer zonaEmp = zonaEmpleado(datos.getString(EMPLEADO_ID));
        System.out.println("Zona emp: " + zonaEmp);
        if (zonaEmp != null && zonaEmp == -1) {

            short result = MAL;
            DataTable[] fragmentos;
            List<Interfaces> sitios = new ArrayList<>();
            fragmentos = datos.fragmentarVertical(FRAG_DATOS, FRAG_LLAVES);
            datos.rewind();
            datos.next();
            if (datos.getInt("adscripcion_id") != 2) {
                //Insert en sitio 1 y 2

                result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                        fragmentos[DATOS]) != null ? BIEN : MAL;
                System.out.println("Sitio 1: " + result);
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                        fragmentos[LLAVES]) != null ? BIEN : MAL;
                System.out.println("Sitio 2: " + result);

                sitios.add(Interfaces.SITIO_1);
                sitios.add(Interfaces.SITIO_2);
            } else {
                //Zona 3 (Local)
                Map<String, Object> condicion = new HashMap<>();
                condicion.put(PLANTEL_ID, datos.getInt(EMPLEADO_PLANTEL_ID));

                DataTable plantel = QueryManager.uniGet(Interfaces.LOCALHOST,
                        PLANTEL, null, null, condicion, PLANTEL_ID);

                //se verifica en su nodo si se encuentra el plantel al que se insertara
                // cambiar por sus nodos el nombre de la variable de sitio y la interface
                if (plantel != null && !plantel.isEmpty()) {
                    //este es su nodo ya no lo inserten de nuevo
                    result = QueryManager.localInsert(false, EMPLEADO,
                            fragmentos[DATOS]) != null ? BIEN : MAL;

                    result *= QueryManager.uniInsert(false, Interfaces.SITIO_5,
                            EMPLEADO, fragmentos[LLAVES]) != null ? BIEN : MAL;

                    result *= QueryManager.uniInsert(false, Interfaces.SITIO_6,
                            EMPLEADO, fragmentos[LLAVES]) != null ? BIEN : MAL;

                    sitios.add(Interfaces.LOCALHOST);
                    sitios.add(Interfaces.SITIO_5);
                    sitios.add(Interfaces.SITIO_6);

                } else {
//                    revisar en los demas nodos
//                     tienen que verificar en los demas nodos en un solo sitio si se encuentra el plantel
//                     aqui se verifica la zona 1
//                    busca en la zona 1 si se encuentra el platel

                    plantel = QueryManager.uniGet(Interfaces.SITIO_2, PLANTEL,
                            null, null, condicion, PLANTEL_ID);

                    if (plantel != null && !plantel.isEmpty()) {
                        //aqui se encuentra

                        result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                                fragmentos[DATOS]) != null ? BIEN : MAL;

                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                                fragmentos[LLAVES]) != null ? BIEN : MAL;

                        sitios.add(Interfaces.SITIO_1);
                        sitios.add(Interfaces.SITIO_2);

                    } else {
//                        aqui se veririca la zona 2

                        plantel = QueryManager.uniGet(Interfaces.SITIO_3, PLANTEL,
                                null, null, condicion, PLANTEL_ID);

                        if (plantel != null && !plantel.isEmpty()) {

                            result = QueryManager.uniInsert(false, Interfaces.SITIO_3, EMPLEADO,
                                    fragmentos[LLAVES]) != null ? BIEN : MAL;

                            result *= QueryManager.uniInsert(false, Interfaces.SITIO_4, EMPLEADO,
                                    fragmentos[DATOS]) != null ? BIEN : MAL;

                            sitios.add(Interfaces.SITIO_3);
                            sitios.add(Interfaces.SITIO_4);

                        }
                    }
                }
            }
            if (result == BIEN) {
                commit(sitios);
            } else {
                ok = false;
                rollback(sitios);
            }
        } else {
            ok = false;
            System.out.println("Empleado id existe");
        }
        System.out.println("Insert empleado: " + ok);
        System.out.println("---------End Insert Empleado transaction----------");
        return ok;
    }

    //Modificar para su sitio
    public static boolean insertPlantel(DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Plantel transaction---------- ");

        short result = MAL;
        datos.rewind();
        datos.next();
        List<Interfaces> sitios = new ArrayList<>();

        Integer siguienteID = obtenerSiguienteID(PLANTEL, PLANTEL_ID, Interfaces.SITIO_1,
                Interfaces.SITIO_4, Interfaces.LOCALHOST);

        if (siguienteID > 0) {
            datos.rewind();
            datos.next();
            datos.setObject(PLANTEL_ID, siguienteID);

            if (null != datos.getInt("zona_id")) {
                switch (datos.getInt("zona_id")) {
                    case 1:
                        System.out.println("Zona 1");
                        result = QueryManager.uniInsert(false, Interfaces.SITIO_2, PLANTEL, datos)
                                != null ? BIEN : MAL;
                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_1, PLANTEL, datos)
                                != null ? BIEN : MAL;

                        sitios.add(Interfaces.SITIO_1);
                        sitios.add(Interfaces.SITIO_2);
                        break;
                    case 2:
                        System.out.println("Zona 2");
                        result = QueryManager.uniInsert(false, Interfaces.SITIO_3, PLANTEL, datos)
                                != null ? BIEN : MAL;
                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_4, PLANTEL, datos)
                                != null ? BIEN : MAL;

                        sitios.add(Interfaces.SITIO_3);
                        sitios.add(Interfaces.SITIO_4);
                        break;
                    case 3:
                        System.out.println("Zona 3");
                        result = QueryManager.uniInsert(false, Interfaces.SITIO_5, PLANTEL, datos)
                                != null ? BIEN : MAL;
                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, PLANTEL, datos)
                                != null ? BIEN : MAL;
                        result *= QueryManager.localInsert(false, PLANTEL, datos)
                                != null ? BIEN : MAL;

                        sitios.add(Interfaces.SITIO_5);
                        sitios.add(Interfaces.SITIO_6);
                        sitios.add(Interfaces.LOCALHOST);
                        break;
                }
            }

            if (result == MAL) {
                ok = false;
                rollback(sitios);
            } else {
                commit(sitios);
            }
        }
        System.out.println("insert plantel: " + ok);
        System.out.println("---------End Plantel transaction----------");
        return ok;
    }

    /**
     * Retorna verdadero si existe el id del empleado, falso de otra forma.
     *
     * @param datos
     * @return
     */
    public static boolean existeEmpleado(DataTable datos) {
        boolean ok;
        Map<String, Object> condicion = new HashMap<>();
        datos.rewind();
        datos.next();
        condicion.put(EMPLEADO_ID, datos.getString(EMPLEADO_ID));

        try {
            //Localhost (Zona 3)
            ok = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO, null, null,
                    condicion, EMPLEADO_ID).next();
            if (!ok) {
                //Zona 1
                ok = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO, null, null,
                        condicion, EMPLEADO_ID).next();
                if (!ok) {
                    //Zona 2
                    ok = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO, null, null,
                            condicion, EMPLEADO_ID).next();
                }
            }

        } catch (NullPointerException e) {
            System.out.println("NullPointer uniGet verificarExistenciaEmpleado");
            ok = true;
        }
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

    //Modificar para su sitio
    public static boolean updateEmpleado(DataTable datos,
            Map<String, ?> attrWhere) {
        System.out.println("---------Start Update Empleado transaction---------- ");
        boolean ok = true;

        System.out.println(datos.toString());
        System.out.println(datos.getRowCount());
        datos.next();
        String idEmpleado = datos.getString(EMPLEADO_ID);

        Integer zona = zonaEmpleado(idEmpleado);

        if (zona != null) {
            List<Interfaces> interfaces = new ArrayList<>();

            switch (zona) {
                case 1:
                    interfaces.add(Interfaces.SITIO_1);
                    interfaces.add(Interfaces.SITIO_2);

                    if (QueryManager.multiDelete(EMPLEADO, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]))) {
                        ok = insertEmpleado(datos);
                    } else {
                        System.out.println("No se pudo eliminar empleado, no se completo modificación");
                        ok = false;
                        rollback(interfaces);
                    }
                    break;
                case 2:
                    interfaces.add(Interfaces.SITIO_3);
                    interfaces.add(Interfaces.SITIO_4);

                    if (QueryManager.multiDelete(EMPLEADO, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]))) {
                        ok = insertEmpleado(datos);
                    } else {
                        System.out.println("No se pudo eliminar empleado, no se completo modificación");
                        ok = false;
                        rollback(interfaces);
                    }
                    break;
                case 3:
                    interfaces.add(Interfaces.SITIO_5);
                    interfaces.add(Interfaces.SITIO_6);
                    interfaces.add(Interfaces.LOCALHOST);

                    if (QueryManager.multiDelete(EMPLEADO, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]))) {
                        ok = insertEmpleado(datos);
                    } else {
                        System.out.println("No se pudo eliminar empleado, no se completo modificación");
                        ok = false;
                        rollback(interfaces);
                    }
                    break;
                case -1:
                    //No existe el empleado, por lo tanto hay un error con este
                    //pseudo update
                    ok = false;
                    break;
            }
        } else {
            ok = false;
        }

        System.out.println("--------- Update Empleado: " + ok);
        System.out.println("---------End Update Empleado transaction---------- ");
        return ok;
    }

    public static boolean updatePlantel(DataTable datos, Map condiciones) {
        Boolean ok = false;

        System.out.println("---------Start Plantel transaction---------- ");

        short result = MAL;
        datos.rewind();
        datos.next();
        List<Interfaces> inter = new ArrayList<>();

        if (null != datos.getInt("zona_id")) {
            switch (datos.getInt("zona_id")) {
                case 1:
                    System.out.println("Zona 1");
                    ok = QueryManager.uniUpdate(Interfaces.SITIO_2, PLANTEL, datos,
                            condiciones);
                    result = ok != null ? BIEN : MAL;

                    result *= QueryManager.uniUpdate(Interfaces.SITIO_1, PLANTEL, datos,
                            condiciones) != null ? BIEN : MAL;

                    inter.add(Interfaces.SITIO_1);
                    inter.add(Interfaces.SITIO_2);
                    break;
                case 2:
                    System.out.println("Zona 2");

                    ok = QueryManager.uniUpdate(Interfaces.SITIO_3, PLANTEL, datos,
                            condiciones);
                    result = ok != null ? BIEN : MAL;
                    result *= QueryManager.uniUpdate(Interfaces.SITIO_4, PLANTEL, datos,
                            condiciones) != null ? BIEN : MAL;

                    inter.add(Interfaces.SITIO_3);
                    inter.add(Interfaces.SITIO_4);
                    break;
                case 3:
                    System.out.println("Zona 3");
                    ok = QueryManager.localUpdate(PLANTEL, datos, condiciones);

                    result = ok != null ? BIEN : MAL;

                    result *= QueryManager.uniUpdate(Interfaces.SITIO_5, PLANTEL, datos,
                            condiciones) != null ? BIEN : MAL;

                    result *= QueryManager.uniUpdate(Interfaces.SITIO_6, PLANTEL, datos,
                            condiciones) != null ? BIEN : MAL;

                    inter.add(Interfaces.SITIO_5);
                    inter.add(Interfaces.SITIO_6);
                    inter.add(Interfaces.LOCALHOST);
                    break;
            }
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

    //Modificar para su sitio
    public static boolean deleteEmpleado(Map<String, Object> attrWhere) {
        System.out.println("---------Start Delete Empleado transaction---------- ");
        boolean ok = true;

        String numeroEmpleado = attrWhere.get(EMPLEADO_ID).toString();

        Integer zonaEmpleado = zonaEmpleado(numeroEmpleado);

        if (zonaEmpleado == null || zonaEmpleado == -1) {
            return false;
        }
        
        List<Interfaces> interfaces = new ArrayList<>();

        switch (zonaEmpleado) {
            case 1:
                interfaces.add(Interfaces.SITIO_1);
                interfaces.add(Interfaces.SITIO_2);

                ok = QueryManager.multiDelete(EMPLEADO, attrWhere,
                        interfaces.toArray(new Interfaces[interfaces.size()]));
                break;
            case 2:
                interfaces.add(Interfaces.SITIO_3);
                interfaces.add(Interfaces.SITIO_4);

                ok = QueryManager.multiDelete(EMPLEADO, attrWhere,
                        interfaces.toArray(new Interfaces[interfaces.size()]));
                break;
            case 3:
                interfaces.add(Interfaces.SITIO_5);
                interfaces.add(Interfaces.SITIO_6);
                interfaces.add(Interfaces.LOCALHOST);

                ok = QueryManager.multiDelete(EMPLEADO, attrWhere,
                        interfaces.toArray(new Interfaces[interfaces.size()]));
                break;
        }
        
        if (ok) {
            commit(interfaces);
        } else {
            ok = false;
            rollback(interfaces);
        }

        System.out.println("--------- Delete Empleado: " + ok);
        System.out.println("---------End Delete Empleado transaction---------- ");

        return ok;
    }

    //Modificar para su sitio
    public static boolean deletePlantel(Map<String, ?> attrWhere) {
        System.out.println("---------Start Delete Plantel transaction---------- ");
        boolean ok = true;

        DataTable plantel = getPlantel(attrWhere);

        if (plantel != null && !plantel.isEmpty()) {
            //Si existe el plantel...
            plantel.next();
            int idZona = plantel.getInt(PLANTEL_ZONA_ID);
            List<Interfaces> interfaces = new ArrayList<>();

            switch (idZona) {
                case 1:
                    interfaces.add(Interfaces.SITIO_1);
                    interfaces.add(Interfaces.SITIO_2);

                    ok = QueryManager.multiDelete(PLANTEL, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]));
                    break;
                case 2:
                    interfaces.add(Interfaces.SITIO_3);
                    interfaces.add(Interfaces.SITIO_4);

                    ok = QueryManager.multiDelete(PLANTEL, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]));
                    break;
                case 3:
                    interfaces.add(Interfaces.SITIO_5);
                    interfaces.add(Interfaces.SITIO_6);
                    interfaces.add(Interfaces.LOCALHOST);

                    ok = QueryManager.multiDelete(PLANTEL, attrWhere,
                            interfaces.toArray(new Interfaces[interfaces.size()]));
                    break;
            }

            if (ok) {
                commit(interfaces);
            } else {
                rollback(interfaces);
            }
        } else {
            //Error, deberia encontrarse el plantel
            ok = false;
        }

        System.out.println("--------- Delete Plantel: " + ok);
        System.out.println("---------End Delete Plantel transaction---------- ");

        return ok;
    }

    public static DataTable consultarEmpleados(Map attrWhere) {
        DataTable empleados;

        System.out.println("---------Start GetEmpleados transaction---------- ");

        //Ver que contiene las condiciones para saber a donde dirigirse
        if (attrWhere.containsKey(EMPLEADO_PLANTEL_ID)
                || attrWhere.containsKey(EMPLEADO_CORREO)
                || attrWhere.containsKey(EMPLEADO_ADSCRIPCION_ID)) {
            //Hacer primero el select con las llaves

            //Zona 1
            //Obtener datos de filtro
            //.......
            DataTable fragLlaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragLlaves == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            HashMap<String, DataTable> condicionIN = new HashMap<>();

            //Si no se regreso nada solo unir a otra dataTable vacia
            DataTable fragDatos;
            if (!fragLlaves.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragLlaves.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona1 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Zona 2
            //Obtener datos de filtro
            fragLlaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragLlaves == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragLlaves.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragLlaves.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragDatos = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona2 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Zona 3
            //Obtener datos de filtro
            fragLlaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragLlaves == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragLlaves.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragLlaves.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragDatos = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona3 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Combinar los resultados de las 3 zonas
            empleados = DataTable.combinarFragH(empleadosZona1, empleadosZona2,
                    empleadosZona3);
        } else {
            //Hacer primero el select con los datos

            //Zona 1
            //Obtener datos de filtro
            //.......
            DataTable fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragDatos == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            HashMap<String, DataTable> condicionIN = new HashMap<>();

            //Si no se regreso nada solo unir a otra dataTable vacia
            DataTable fragLlaves;
            if (!fragDatos.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragDatos.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona1 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Zona 2
            //Obtener datos de filtro
            //.......
            fragDatos = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragDatos == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragDatos.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragDatos.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona2 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Zona 3
            //Obtener datos de filtro
            //.......
            fragDatos = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO,
                    null, null, attrWhere, EMPLEADO_ID);

            //Si hay error regresar error
            if (fragDatos == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragDatos.isEmpty()) {
                condicionIN.put(EMPLEADO_ID + " IN",
                        fragDatos.obtenerColumnas(new String[]{EMPLEADO_ID}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                        null, null, condicionIN, EMPLEADO_ID);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona3 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    EMPLEADO_ID);

            //Combinar los resultados de las 3 zonas
            empleados = DataTable.combinarFragH(empleadosZona1, empleadosZona2,
                    empleadosZona3);

        }

        System.out.println("---------End GetEmpleados transaction---------- ");

        return empleados;
    }

    public static DataTable consultarEmpleadosByD(Map attrWhere) {
        DataTable empleados;

        System.out.println("---------Start GetEmpleados transaction---------- ");
        //Hacer primero el select con las llaves

        //Departamento o Direccion
        //Obtener datos de filtro
        //.......
        DataTable fragLlaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                null, null, attrWhere, EMPLEADO_ID);

        //Si hay error regresar error
        if (fragLlaves == null) {
            return null;
        }

        //Obtener el fragmento correspondiente del otro sitio que corresponda
        //con los registros obtenidos
        HashMap<String, DataTable> condicionIN = new HashMap<>();

        //Si no se regreso nada solo unir a otra dataTable vacia
        DataTable fragDatos;
        if (!fragLlaves.isEmpty()) {
            condicionIN.put(EMPLEADO_ID + " IN",
                    fragLlaves.obtenerColumnas(new String[]{EMPLEADO_ID}));

            fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                    null, null, condicionIN, EMPLEADO_ID);
        } else {
            //Si la primera tabla esta vacia no tiene caso buscar en el otro
            //sitio
            fragDatos = new DataTable(FRAG_DATOS, 0, 0);
        }

        //Combinar ambos fragmentos
        empleados = DataTable.combinarFragV(fragDatos, fragLlaves,
                EMPLEADO_ID);

        System.out.println("---------End GetEmpleados transaction---------- ");

        return empleados;
    }

    public static DataTable consultarEmpleados() {
        System.out.println("---------Start GetEmpleados transaction---------- ");

        String[] columnas = {
            "numero",
            "primer_nombre",
            "segundo_nombre",
            "apellido_paterno",
            "apellido_materno"};

        //Zona 1
        DataTable fragDatosZona1 = QueryManager.uniGet(Interfaces.SITIO_1,
                EMPLEADO, columnas, null, null, EMPLEADO_ID);

        DataTable fragDatosZona2 = QueryManager.uniGet(Interfaces.SITIO_4,
                EMPLEADO, columnas, null, null, EMPLEADO_ID);

        DataTable fragDatosZona3 = QueryManager.uniGet(Interfaces.LOCALHOST,
                EMPLEADO, columnas, null, null, EMPLEADO_ID);

        System.out.println("---------End GetEmpleados transaction---------- ");

        return DataTable.combinarFragH(fragDatosZona1, fragDatosZona2, fragDatosZona3);
    }

    public static DataTable consultarPlanteles(Map attrWhere) {

        System.out.println("---------Start GetPlanteles transaction---------- ");

        //Zona 1
        DataTable fragDatosZona1 = QueryManager.uniGet(
                Interfaces.SITIO_1, PLANTEL, null, null, attrWhere, PLANTEL_ID);
        //Zona 2
        DataTable fragDatosZona2 = QueryManager.uniGet(
                Interfaces.SITIO_3, PLANTEL, null, null, attrWhere, PLANTEL_ID);
        //Zona 3
        DataTable fragDatosZona3 = QueryManager.uniGet(
                Interfaces.LOCALHOST, PLANTEL, null, null, attrWhere, PLANTEL_ID);

        System.out.println("---------End GetEmpleados transaction---------- ");

        return DataTable.combinarFragH(fragDatosZona1, fragDatosZona2,
                fragDatosZona3);
    }

    public static DataTable consultarImplementacionesByEmpleado(String numero) {
        DataTable implementaciones = null;

        System.out.println("---------Start GetImplementaciones transaction---------- ");

        Integer zona = zonaEmpleado(numero);

        if (zona != null && zona != -1) {
            //El empleado existe, ahora a buscar sus implementaciones
            HashMap<String, String> condicion = new HashMap<>();
            condicion.put(EMPLEADO_NUMERO, numero);

            switch (zona) {
                case 1:
                    implementaciones = QueryManager.uniGet(Interfaces.SITIO_2,
                            IMPLEMENTACION_EVENTO_EMPLEADO,
                            new String[]{IMPLEMENTACION_EVENTO_ID},
                            new String[]{null}, condicion,
                            IMPLEMENTACION_EVENTO_ID);
                    break;

                case 2:
                    implementaciones = QueryManager.uniGet(Interfaces.SITIO_4,
                            IMPLEMENTACION_EVENTO_EMPLEADO,
                            new String[]{IMPLEMENTACION_EVENTO_ID},
                            new String[]{null}, condicion,
                            IMPLEMENTACION_EVENTO_ID);
                    break;

                case 3:
                    implementaciones = QueryManager.uniGet(Interfaces.LOCALHOST,
                            IMPLEMENTACION_EVENTO_EMPLEADO,
                            new String[]{IMPLEMENTACION_EVENTO_ID},
                            new String[]{null}, condicion,
                            IMPLEMENTACION_EVENTO_ID);
                    break;
            }
        } else {
            implementaciones = null;
        }

        System.out.println("---------End GetImplementaciones transaction---------- ");

        return implementaciones;
    }

    public static DataTable getEmpleado(String[] columnas, Map<String, ?> condicion) {
        System.out.println("---------Start GetEmpleado transaction---------- ");

        String[] fragLlaves = {"numero", "correo", "adscripcion_id",
            "departamento_id", "plantel_id", "direccion_id"};
        List<String> listaLlaves = Arrays.asList(fragLlaves);
        List<String> listaColumnas = null;
        if (columnas != null) {
            listaColumnas = Arrays.asList(columnas);
        }

        //Buscar en la zona 3...
        DataTable empleado = QueryManager.uniGet(Interfaces.LOCALHOST,
                EMPLEADO, columnas, null, condicion, EMPLEADO_ID);
        if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
            DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                    columnas, null, condicion, EMPLEADO_ID);
            empleado = DataTable.combinarFragV(empleado, llaves, EMPLEADO_ID);
        }

        if (empleado == null || empleado.isEmpty()) {
            //Zona 2
            empleado = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO, null,
                    null, condicion, EMPLEADO_ID);
            if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
                DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                        columnas, null, condicion, EMPLEADO_ID);
                empleado = DataTable.combinarFragV(empleado, llaves, EMPLEADO_ID);
            }

            if (empleado == null || empleado.isEmpty()) {
                //Zona 1
                empleado = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO, null,
                        null, condicion, EMPLEADO_ID);
                if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
                    DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                            columnas, null, condicion, EMPLEADO_ID);
                    empleado = DataTable.combinarFragV(empleado, llaves, EMPLEADO_ID);
                }
            }
        }

        System.out.println("---------End GetEmpleado transaction----------");
        return empleado;
    }

    public static DataTable getPlantel(Map<String, ?> condicion) {
        System.out.println("---------Start GetPlantel transaction---------- ");

        //Se busca en el nodo local (Zona 3)
        DataTable plantel = QueryManager.uniGet(Interfaces.LOCALHOST,
                PLANTEL, null, null, condicion, PLANTEL_ID);

        if (plantel == null || plantel.isEmpty()) {
            //Si no se encontró en la Zona 3 buscar en la Zona 2
            plantel = QueryManager.uniGet(Interfaces.SITIO_3, PLANTEL, null, null,
                    condicion, PLANTEL_ID);

            if (plantel == null || plantel.isEmpty()) {
                //Si no esta en la Zona 2 buscar en la Zona 1
                plantel = QueryManager.uniGet(Interfaces.SITIO_1, PLANTEL,
                        null, null, condicion, PLANTEL_ID);

                //Si no se encontró aquí regresar el plantel vacío de todos modos
            }
        }

        System.out.println("---------End GetPlantel transaction----------");
        return plantel;
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

    /**
     * Retorna el número de la zona a la que pertenece, -1 si no existe el
     * empleado, null si hay problemas al obtener la información.
     *
     * @param numero
     * @return
     */
    //Modificar para su sitio
    public static Integer zonaEmpleado(String numero) {
        boolean ok;
        Integer zona = -1;
        Map<String, Object> condicion = new HashMap<>();
        condicion.put(EMPLEADO_ID, numero);

        try {
            //Zona 3
            ok = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO, null, null,
                    condicion, EMPLEADO_ID).next();
            if (!ok) {
                //Zona 1
                ok = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO, null, null,
                        condicion, EMPLEADO_ID).next();
                if (!ok) {
                    //Zona 2
                    ok = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO, null,
                            null, condicion, EMPLEADO_ID).next();
                    if (ok) {
                        zona = 2;
                    }
                } else {
                    zona = 1;
                }
            } else {
                zona = 3;
            }

        } catch (NullPointerException e) {
            System.out.println("NullPointer uniGet zonaEmpleado");
            zona = null;
        }

        return zona;
    }

    private static int obtenerSiguienteID(String tabla, String columnaID,
            Interfaces... interfacesSitios) {

        int mayor = -1;
        int idSitio;
        for (Interfaces interfaceSitio : interfacesSitios) {
            idSitio = QueryManager.getMaxId(interfaceSitio, tabla, columnaID);
            if (idSitio > mayor) {
                mayor = idSitio;
            }
        }

        return ++mayor;
    }

}
