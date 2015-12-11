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

    private static final String COLUMNA_ID_EMPLEADO = "numero";
    private static final String EMPLEADO_PLANTEL_ID = "plantel_id";
    private static final String EMPLEADO_CORREO = "correo";
    private static final String EMPLEADO_ADSCRIPCION_ID = "adscripcion_id";
    private static final String[] FRAG_LLAVES = {COLUMNA_ID_EMPLEADO,
        EMPLEADO_CORREO, EMPLEADO_ADSCRIPCION_ID, "departamento_id", EMPLEADO_PLANTEL_ID, "direccion_id"};

    private static final String[] FRAG_DATOS = {COLUMNA_ID_EMPLEADO, "primer_nombre", "segundo_nombre",
        "apellido_paterno", "apellido_materno", "puesto_id"};
    private static final String COLUMNA_ID_PLANTEL = "id";
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

        System.out.println("---------Start Insert Empleado transaction---------- ");

        if (!existeEmpleado(datos)) {

            short result = MAL;
            DataTable[] fragmentos;
            datos.rewind();
            datos.next();
            List<Interfaces> sitios = new ArrayList<>();
            fragmentos = datos.fragmentarVertical(FRAG_DATOS, FRAG_LLAVES);
            datos.rewind();
            datos.next();
            if (datos.getInt("adscripcion_id") != 2) {
                //Insert en sitio 1 y 2

                result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                        fragmentos[NOMBRES]) != null ? BIEN : MAL;
                System.out.println("Sitio 1: " + result);
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                        fragmentos[LLAVES]) != null ? BIEN : MAL;
                System.out.println("Sitio 2: " + result);

                sitios.add(Interfaces.SITIO_1);
                sitios.add(Interfaces.SITIO_2);
            } else {

                Map<String, Object> condicion = new HashMap<>();
                condicion.put(COLUMNA_ID_PLANTEL, datos.getInt("plantel_id"));

                DataTable plantel = QueryManager.uniGet(Interfaces.LOCALHOST,
                        PLANTEL, null, null, condicion, COLUMNA_ID_PLANTEL);

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

                    sitios.add(Interfaces.LOCALHOST);
                    sitios.add(Interfaces.SITIO_4);

                } else {
//                    revisar en los demas nodos
//                     tienen que verificar en los demas nodos en un solo sitio si se encuentra el plantel
//                     aqui se verifica la zona 1
//                    busca en la zona 1 si se encuentra el platel

                    plantel = QueryManager.uniGet(Interfaces.SITIO_2, PLANTEL,
                            null, null, condicion, COLUMNA_ID_PLANTEL);

                    if (plantel != null && plantel.getRowCount() != 0) {
                        //aqui se encuentra

                        result = QueryManager.uniInsert(false, Interfaces.SITIO_1, EMPLEADO,
                                fragmentos[NOMBRES]) != null ? BIEN : MAL;
                        System.out.println("Sitio 1: " + result);

                        result *= QueryManager.uniInsert(false, Interfaces.SITIO_2, EMPLEADO,
                                fragmentos[LLAVES]) != null ? BIEN : MAL;

                        System.out.println("Sitio 2: " + result);

                        sitios.add(Interfaces.SITIO_1);
                        sitios.add(Interfaces.SITIO_2);

                    } else {
//                        aqui se veririca la zona 3

                        plantel = QueryManager.uniGet(Interfaces.SITIO_7, PLANTEL,
                                null, null, condicion, COLUMNA_ID_PLANTEL);

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

                            sitios.add(Interfaces.SITIO_5);
                            sitios.add(Interfaces.SITIO_6);
                            sitios.add(Interfaces.SITIO_7);

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
        condicion.put(COLUMNA_ID_EMPLEADO, datos.getString(COLUMNA_ID_EMPLEADO));

        try {
            ok = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO, null, null,
                    condicion, COLUMNA_ID_EMPLEADO).next();
            if (!ok) {
                ok = QueryManager.uniGet(Interfaces.SITIO_7, EMPLEADO, null, null,
                        condicion, COLUMNA_ID_EMPLEADO).next();
                if (!ok) {
                    ok = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO, null, null,
                            condicion, COLUMNA_ID_EMPLEADO).next();
                }
            }

        } catch (NullPointerException e) {
            System.out.println("NullPointer uniGet verificarExistenciaEmpleado");
            ok = true;
        }
        return ok;
    }

    public static boolean insertPlantel(DataTable datos) {
        boolean ok = true;

        System.out.println("---------Start Plantel transaction---------- ");

        short result = MAL;
        datos.rewind();
        datos.next();
        List<Interfaces> sitios = new ArrayList<>();

        Integer siguienteID = obtenerSiguienteID(PLANTEL, COLUMNA_ID_PLANTEL, Interfaces.SITIO_1,
                Interfaces.LOCALHOST, Interfaces.SITIO_7);

        if (siguienteID > 0) {
            datos.rewind();
            datos.next();
            datos.setObject(COLUMNA_ID_PLANTEL, siguienteID);

            if (datos.getInt("zona_id") == 1) {

                System.out.println("Zona 1");
                result = QueryManager.uniInsert(false, Interfaces.SITIO_2, PLANTEL, datos)
                        != null ? BIEN : MAL;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_1, PLANTEL, datos)
                        != null ? BIEN : MAL;

                sitios.add(Interfaces.SITIO_1);
                sitios.add(Interfaces.SITIO_2);

            } else if (datos.getInt("zona_id") == 2) {

                System.out.println("Zona 2");
                result = QueryManager.localInsert(false, PLANTEL, datos)
                        != null ? BIEN : MAL;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_4, PLANTEL, datos)
                        != null ? BIEN : MAL;

                sitios.add(Interfaces.LOCALHOST);
                sitios.add(Interfaces.SITIO_4);

            } else if (datos.getInt("zona_id") == 3) {

                System.out.println("Zona 3");
                result = QueryManager.uniInsert(false, Interfaces.SITIO_5, PLANTEL, datos)
                        != null ? BIEN : MAL;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_6, PLANTEL, datos)
                        != null ? BIEN : MAL;
                result *= QueryManager.uniInsert(false, Interfaces.SITIO_7, PLANTEL, datos)
                        != null ? BIEN : MAL;

                sitios.add(Interfaces.SITIO_5);
                sitios.add(Interfaces.SITIO_6);
                sitios.add(Interfaces.SITIO_7);
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
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

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
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragLlaves.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona1 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

            //Zona 2
            //Obtener datos de filtro
            fragLlaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

            //Si hay error regresar error
            if (fragLlaves == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragLlaves.isEmpty()) {
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragLlaves.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragDatos = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona2 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

            //Zona 3
            //Obtener datos de filtro
            fragLlaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

            //Si hay error regresar error
            if (fragLlaves == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragLlaves.isEmpty()) {
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragLlaves.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragDatos = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragDatos = new DataTable(FRAG_DATOS, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona3 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

            //Combinar los resultados de las 3 zonas
            empleados = DataTable.combinarFragH(empleadosZona1, empleadosZona2,
                    empleadosZona3);
        } else {
            //Hacer primero el select con los datos

            //Zona 1
            //Obtener datos de filtro
            //.......
            DataTable fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

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
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragDatos.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona1 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

            //Zona 2
            //Obtener datos de filtro
            //.......
            fragDatos = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO,
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

            //Si hay error regresar error
            if (fragDatos == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragDatos.isEmpty()) {
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragDatos.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona2 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

            //Zona 3
            //Obtener datos de filtro
            //.......
            fragDatos = QueryManager.uniGet(Interfaces.LOCALHOST, EMPLEADO,
                    null, null, attrWhere, COLUMNA_ID_EMPLEADO);

            //Si hay error regresar error
            if (fragDatos == null) {
                return null;
            }

            //Obtener el fragmento correspondiente del otro sitio que corresponda
            //con los registros obtenidos
            condicionIN.clear();

            //Si no se regreso nada solo unir a otra dataTable vacia
            if (!fragDatos.isEmpty()) {
                condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                        fragDatos.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

                fragLlaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                        null, null, condicionIN, COLUMNA_ID_EMPLEADO);
            } else {
                //Si la primera tabla esta vacia no tiene caso buscar en el otro
                //sitio
                fragLlaves = new DataTable(FRAG_LLAVES, 0, 0);
            }

            //Combinar ambos fragmentos
            DataTable empleadosZona3 = DataTable.combinarFragV(fragDatos, fragLlaves,
                    COLUMNA_ID_EMPLEADO);

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
                null, null, attrWhere, COLUMNA_ID_EMPLEADO);

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
            condicionIN.put(COLUMNA_ID_EMPLEADO + " IN",
                    fragLlaves.obtenerColumnas(new String[]{COLUMNA_ID_EMPLEADO}));

            fragDatos = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO,
                    null, null, condicionIN, COLUMNA_ID_EMPLEADO);
        } else {
            //Si la primera tabla esta vacia no tiene caso buscar en el otro
            //sitio
            fragDatos = new DataTable(FRAG_DATOS, 0, 0);
        }

        //Combinar ambos fragmentos
        empleados = DataTable.combinarFragV(fragDatos, fragLlaves,
                COLUMNA_ID_EMPLEADO);

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
                EMPLEADO, columnas, null, null, COLUMNA_ID_EMPLEADO);

        DataTable fragDatosZona2 = QueryManager.uniGet(Interfaces.SITIO_4,
                EMPLEADO, columnas, null, null, COLUMNA_ID_EMPLEADO);

        DataTable fragDatosZona3 = QueryManager.uniGet(Interfaces.LOCALHOST,
                EMPLEADO, columnas, null, null, COLUMNA_ID_EMPLEADO);

        System.out.println("---------End GetEmpleados transaction---------- ");

        return DataTable.combinarFragH(fragDatosZona1, fragDatosZona2, fragDatosZona3);
    }

    public static DataTable consultarPlanteles(Map attrWhere) {

        System.out.println("---------Start GetPlanteles transaction---------- ");

        //Zona 1
        DataTable fragDatosZona1 = QueryManager.uniGet(
                Interfaces.SITIO_1, PLANTEL, null, null, attrWhere, COLUMNA_ID_PLANTEL);
        //Zona 2
        DataTable fragDatosZona2 = QueryManager.uniGet(
                Interfaces.SITIO_3, PLANTEL, null, null, attrWhere, COLUMNA_ID_PLANTEL);
        //Zona 3
        DataTable fragDatosZona3 = QueryManager.uniGet(
                Interfaces.LOCALHOST, PLANTEL, null, null, attrWhere, COLUMNA_ID_PLANTEL);

        System.out.println("---------End GetEmpleados transaction---------- ");

        return DataTable.combinarFragH(fragDatosZona1, fragDatosZona2,
                fragDatosZona3);
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
                EMPLEADO, columnas, null, condicion, COLUMNA_ID_EMPLEADO);
        if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
            DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_5, EMPLEADO,
                    columnas, null, condicion, COLUMNA_ID_EMPLEADO);
            empleado = DataTable.combinarFragV(empleado, llaves, COLUMNA_ID_EMPLEADO);
        }

        if (empleado == null || empleado.isEmpty()) {
            //Zona 2
            empleado = QueryManager.uniGet(Interfaces.SITIO_4, EMPLEADO, null,
                    null, condicion, COLUMNA_ID_EMPLEADO);
            if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
                DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_3, EMPLEADO,
                        columnas, null, condicion, COLUMNA_ID_EMPLEADO);
                empleado = DataTable.combinarFragV(empleado, llaves, COLUMNA_ID_EMPLEADO);
            }

            if (empleado == null || empleado.isEmpty()) {
                //Zona 1
                empleado = QueryManager.uniGet(Interfaces.SITIO_1, EMPLEADO, null,
                        null, condicion, COLUMNA_ID_EMPLEADO);
                if (columnas == null || listaLlaves.retainAll(listaColumnas)) {
                    DataTable llaves = QueryManager.uniGet(Interfaces.SITIO_2, EMPLEADO,
                            columnas, null, condicion, COLUMNA_ID_EMPLEADO);
                    empleado = DataTable.combinarFragV(empleado, llaves, COLUMNA_ID_EMPLEADO);
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
                PLANTEL, null, null, condicion, COLUMNA_ID_PLANTEL);

        if (plantel == null || plantel.isEmpty()) {
            //Si no se encontró en la Zona 3 buscar en la Zona 2
            plantel = QueryManager.uniGet(Interfaces.SITIO_3, PLANTEL, null, null,
                    condicion, COLUMNA_ID_PLANTEL);

            if (plantel == null || plantel.isEmpty()) {
                //Si no esta en la Zona 2 buscar en la Zona 1
                plantel = QueryManager.uniGet(Interfaces.SITIO_1, PLANTEL,
                        null, null, condicion, COLUMNA_ID_PLANTEL);

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

}
