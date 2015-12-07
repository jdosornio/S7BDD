/*
 * Copyright (C) 2015 Jesús Donaldo Osornio Hernández
 *
 * This file is part of MatExámenes.
 *
 * MatExámenes is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * MatExámenes is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package remote.util;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import remote.Sitio;

/**
 * Esta clase se encarga de hacer la conexión remota con un registro en la
 * aplicación de persistencia por medio de la interface Persistencia
 *
 * @author Jesus Donaldo Osornio Hernández
 * @version 1 18 Mayo 2015
 */
public class InterfaceManager {

    /**
     * Los posibles valores de los sitios. Del 1 - 7 y localhost
     */
    public static enum Interfaces {
        SITIO_1, SITIO_2, SITIO_3, SITIO_4,
        SITIO_5, SITIO_6, SITIO_7, LOCALHOST
    }
    /**
     * Guarda los nombres de la interface de cada sitio (no muy necesario, sólo
     * si se desean cambiar los nombres desde la interfaz gráfica)
     */
    private static final Map<Interfaces, String> SERVICIOS_SITIO = new HashMap<>();
    /**
     * Guarda la interface del sitio por el nombre de su interface
     */
    private static final Map<String, Sitio> SITIOS = new HashMap<>();
    
    
    public static void addInterface(String ip, int puerto, String servicio)
            throws RemoteException, NotBoundException {
        
        if(!SITIOS.containsKey(servicio)) {
            Registry registro = LocateRegistry.getRegistry(ip, puerto);
            Sitio sitio = (Sitio) registro.lookup(servicio);
            
            SITIOS.put(servicio, sitio);
            
            System.out.println("Inteface added: " + servicio + " ip: " + ip +
                            " en puerto: " + puerto);
        }
    }
    
    public static Sitio getInterface(String servicio)
            throws RemoteException, NotBoundException {
     
        return SITIOS.get(servicio);
    }
    
    /**
     * Guarda el nombre de la interface de servicio de cada sitio y localhost
     * @param interfaceSitio el valor del enum Interfaces que representa el sitio
     * que se desea guardar
     * @param nombre el nombre de la interface de servicio
     */
    public static void setInterfaceServicio(Interfaces interfaceSitio, String nombre) {
        SERVICIOS_SITIO.put(interfaceSitio, nombre);
        System.out.println("Interface set: " + interfaceSitio + " con nombre: " +
                nombre);
    }
    
    /**
     * Obtiene el nombre de la interface de servicio de cada sitio y localhost
     * 
     * @param interfaceSitio el valor del enum Interfaces que representa el sitio
     * del que se desea obtener el nombre de su interface
     * @return 
     */
    public static String getInterfaceServicio(Interfaces interfaceSitio) {
        return SERVICIOS_SITIO.get(interfaceSitio);
    }
}