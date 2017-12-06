/**
 *@name CopiarArchivosCuadraturaJob2.java
 * 
 *@version 1.0 
 * 
 *@date 30-03-2017
 * 
 *@author EA7129
 * 
 *@copyright Cencosud. All rights reserved.
 */
package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
//import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Tenemos que trabajar en un nuevo desarrollo donde se muestren la diferencia. 
 * Los casos que se deben mostrar son los TipoVenta = 'VeV PD'  
 * con tiempo de creación mayor a una hora tengan N_OrdenDistribu en '0' o vacío. 
 * Si es distinto a 0 quiere decir que no tiene la orden de distribuicion no se ha enviado a B2B
 * La razón de esto es porque para las ordenes de VEV PD deberían tener orden de distribución pasada una hora de la creación pero hay casos que no. 
 * 
 */
public class EjecutarBatchJobVentaEnVerdePd {
	
	private static final int DIFF_HOY_FECHA_INI = 8;
	private static final int DIFF_HOY_FECHA_FIN = 1;
	//private static final int FORMATO_FECHA_0 = 0;
	//private static final int FORMATO_FECHA_1 = 1;
	//private static final int FORMATO_FECHA_3 = 3;

	private static BufferedWriter bw;
	private static String path;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			} else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info = null;
			File miDir = new File(".");
			path = miDir.getCanonicalPath();
			// path = "C:\\PROYECTOS\\C8INVENTARIOS\\MASIVOS\\";
			// info = new File(path+"/LOG/info.txt");
			info = new File(path + "/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		} catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map<String, String> mapArguments) {
		// TODO Auto-generated method stub
		Connection dbconnection = crearConexion();
		Connection dbconnOracle2 = crearConexionOracle2();
		//Connection dbconnOracle = crearConexionOracle();

		
		File file1              							= null;
		BufferedWriter bw       							= null;
		PreparedStatement pstmt 							= null;
		PreparedStatement pstmtVentataVerde 				= null;
		StringBuffer sb        								= null;
		String sFechaIni        = null;
		String sFechaFin        = null;		
		//Date now3 = new Date();
		//SimpleDateFormat ft3 = new SimpleDateFormat ("YYYYMMdd");
		//String iFechaFin = ft3.format(now3);
		
		
		//Date nowPro = new Date();
		//SimpleDateFormat ftPro = new SimpleDateFormat ("YYYY-MM-dd HH:mm:ss");
		//String currentDatePro = ftPro.format(nowPro);
		
		
		//String fechaEom = String.valueOf(restarDia(currentDate3));
		//String fechaEom2 = String.valueOf((currentDate3));
		
		//System.out.println("currentDatePro="+currentDatePro);
		//System.out.println("=1="+fechaEom.substring(6,8)+"-"+fechaEom.substring(4,6)+"-"+fechaEom.substring(0,4));
		//System.out.println("=2="+fechaEom2.substring(6,8)+"-"+fechaEom2.substring(4,6)+"-"+fechaEom2.substring(0,4));
		//System.out.println("=2="+fechaEom2.substring(6,8)+"/"+fechaEom2.substring(4,6)+"/"+fechaEom2.substring(2,4));

		try {

			try {
				sFechaIni = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_INI);
				sFechaFin = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			//System.out.println("currentDate1: " + iFechaFin);
			//System.out.println("currentDate2: " + restarDia(iFechaFin));
			info("[sFechaIni] " +sFechaIni);
			info("[sFechaFin] " +sFechaFin);
				
			Thread.sleep(60);
			info("Pausa para Eliminar CUADRATURA_VTAV_PD sleep(60 seg)");
			elimnarCuadratura(dbconnOracle2,"DELETE FROM  cuadratura_vtav_pd  where 1 = 1 AND fecha_creacion >= '"+sFechaIni+" 00:00:00' AND fecha_creacion <= '"+sFechaFin+" 23:59:59'");
			commit(dbconnOracle2,"COMMIT");

			Thread.sleep(60);
			System.out.println("Regreso del Proceso Venta En Verde PD sleep(60 seg)");
			
			
			file1                   = new File(path + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			sb 												= new StringBuffer();
			
			
			
			//##################### CARGAR TABLA ECOMMERCE SOPORTE VENTA  #####################
			Thread.sleep(60);
			info("Pausa para sleep(60 seg)");
			
			//ejecutarPaso1(dbconnection);
			//ejecutarPaso2(iFechaIni,iFechaFin, dbconnection);
			//ejecutarPaso3(iFechaIni,iFechaFin, dbconnection);
			//ejecutarPaso4(iFechaIni,iFechaFin, dbconnection);
			//ejecutarPaso5(iFechaIni, dbconnection);
			
			String sql = "Insert into cuadratura_vtav_pd (tc_po_line_id,n_solicitud_cliente,n_orden_distribu,fecha_creacion_orden,fechacompromiso,estorden,estLineaorden,estado_od,sku,descsku,cantsku,despacho,tipo_orden, mes, ano, descripcion_tipo_orden, tipo_venta, tipo_numero_distribuicion, horario, horario2, fecha_creacion) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstmtVentataVerde = dbconnOracle2.prepareStatement(sql);
			
			sb = new StringBuffer();
			sb.append("SELECT POL.tc_po_line_id,PO.tc_purchase_orders_id                 AS N_SolicitudCliente ");
			sb.append(",O.tc_order_id                            AS N_OrdenDistribu ");
			sb.append(",To_Char (pol.created_dttm, 'DD-MM-YYYY HH24:MI' )         as Fecha_Creacion_Orden ");
			sb.append(",To_Char (pol.comm_dlvr_dttm, 'dd/MM/yy') AS FechaCompromiso ");
			sb.append(",POS.description                          AS EstOrden ");
			sb.append(",POLS.description                         AS EstLineaOrden ");
			sb.append(",DOS.description                          AS Estado_OD ");
			sb.append(",POL.sku                                  AS SKU ");
			sb.append(",IC.DESCRIPTION                           AS DescSKU ");
			sb.append(",POL.allocated_qty                        AS CantSKU ");
			sb.append(",Max(O.O_FACILITY_ALIAS_ID)               AS DESPACHO ");
			sb.append(",OT.order_type                            AS Tipo_Orden ");
			sb.append(",To_Char (pol.comm_dlvr_dttm, 'MM')       AS Mes ");
			sb.append(",To_Char (pol.comm_dlvr_dttm, 'yyyy')     AS Ano ");
			sb.append(",CASE  WHEN max(ao.a_allocation_type) = 0 THEN CASE WHEN MAX(AO.A_DCNAME) <> '012' AND MAX(AO.A_DCNAME) <> '200' THEN  'VeV PD' ELSE 'Stock CD' END ELSE CASE WHEN (OT.order_type) = 'Pickup' THEN 'Stock CD'  ELSE 'VeV CD' END END as TipoVenta ");
			sb.append(",To_Char (pol.created_dttm, 'HH')     AS hora ");
			//sb.append(",To_Char (pol.comm_dlvr_dttm, 'DD-MM-YYYY HH24:MI:SS' )         as fecha_creacion ");
			sb.append(",To_Char (pol.comm_dlvr_dttm, 'YYYY-MM-DD HH24:MI:SS' )         as fecha_creacion ");
			
			
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("Inner JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id =  po.purchase_orders_id  AND POL.PURCHASE_ORDERS_LINE_STATUS <> 940 ");
			sb.append("Inner JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status =  po.purchase_orders_status AND po.purchase_orders_status <> 492  ");
			sb.append("Inner JOIN CA14.purchase_orders_line_status POLS ON poLS.purchase_orders_line_status =  poL.PURCHASE_ORDERS_LINE_STATUS AND poL.PURCHASE_ORDERS_LINE_STATUS not in (492, 850)   ");
			sb.append("Inner JOIN CA14.order_type ot  ON ot.order_type_id =  po.order_category ");
			sb.append("Inner JOIN CA14.item_cbo ic ON ic.item_id =  pol.sku_id ");
			sb.append("INNER JOIN CA14.a_orderinventoryallocation ao ON ao.a_orderlineid = pol.purchase_orders_line_item_id        ");
			sb.append("left JOIN CA14.ORDER_LINE_ITEM OLI ON OLI.MO_LINE_ITEM_ID = POL.PURCHASE_ORDERS_LINE_ITEM_ID ");
			sb.append("left JOIN CA14.orders o ON o.order_id =  oli.order_id AND o.purchase_order_id = po.purchase_orders_id ");
			sb.append("left JOIN CA14.DO_STATUS DOS ON DOS.ORDER_STATUS = O.DO_STATUS ");
			sb.append("WHERE po.channel_type = 20 AND PO.creation_type =30 AND nvl(O.O_FACILITY_ALIAS_ID, '012') IN ('012', '200', '400') ");
			sb.append("AND pol.comm_dlvr_dttm >= to_date('");
			sb.append(sFechaIni);
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.comm_dlvr_dttm <= to_date('");
			sb.append(sFechaFin);
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("GROUP BY ");
			sb.append("POL.tc_po_line_id,PO.tc_purchase_orders_id,po.created_dttm,O.created_dttm ,O.tc_order_id,pol.comm_dlvr_dttm,POS.description,POLS.description,DOS.description,POL.sku,IC.DESCRIPTION,POL.allocated_qty,pol.total_monetary_value,IC.prod_type,PO.entry_code,O.O_FACILITY_ALIAS_ID,OT.order_type, pol.created_dttm  ");
			sb.append("ORDER BY po.TC_PURCHASE_ORDERS_ID, POL.tc_po_line_id");

			info("[sb = ]" + sb);
			pstmt        = dbconnection.prepareStatement(sb.toString());
			//pstmt.setString(1, sFechaIni);
			//pstmt.setString(2, sFechaFin);
			ResultSet rs = pstmt.executeQuery();
			bw = new BufferedWriter(new FileWriter(file1));
			bw.write("tc_po_line_id;");
			bw.write("N_SolicitudCliente;");
			bw.write("N_OrdenDistribu;");
			bw.write("Fecha_Creacion_Orden;");
			bw.write("FechaCompromiso;");
			bw.write("EstOrden;");
			bw.write("EstLineaOrden;");
			bw.write("Estado_OD;");
			bw.write("SKU;");
			bw.write("DescSKU;");
			bw.write("CantSKU;");
			bw.write("DESPACHO;");
			bw.write("Tipo_Orden;");
			bw.write("Mes;");
			bw.write("Año;");
			bw.write("Descripcion TipoVenta;");
			bw.write("Tipo Venta;");
			bw.write("N_OrdenDistribu;");
			bw.write("Horario;");
			bw.write("Horario2;");
			bw.write("Fecha Creacion\n");

			sb = new StringBuffer();
			
			info("[Ini paso 6]");
			while (rs.next()) {
				//String fechaCreacion  = rs.getString("as Fecha_Creacion_Orden");
				//int numeroOrdenDistribucion  = rs.getInt("N_OrdenDistribu");
				//String tipoVenta 	= rs.getString("TipoVenta");
				
				
				bw.write(rs.getString("tc_po_line_id") + ";");
				bw.write(rs.getString("N_SolicitudCliente") + ";");
				bw.write(rs.getString("N_OrdenDistribu") + ";");
				//bw.write(formatDate(rs.getTimestamp("Fecha_Creacion_Orden"), FORMATO_FECHA_0) + ";");
				//bw.write(formatDate(rs.getTimestamp("FechaCompromiso"), FORMATO_FECHA_0) + ";");
				bw.write(rs.getString("Fecha_Creacion_Orden") + ";");
				bw.write(rs.getString("FechaCompromiso") + ";");
				
				bw.write(rs.getString("EstOrden") + ";");
				bw.write(rs.getString("EstLineaOrden") + ";");
				bw.write(rs.getString("Estado_OD") + ";");
				bw.write(rs.getString("SKU") + ";");
				bw.write(rs.getString("DescSKU") + ";");
				bw.write(rs.getString("CantSKU") + ";");
				bw.write(rs.getString("DESPACHO") + ";");
				bw.write(rs.getString("Tipo_Orden") + ";");
				bw.write(rs.getString("Mes") + ";");
				bw.write(rs.getString("Ano") + ";");
				bw.write(rs.getString("TipoVenta") + ";");
				bw.write(tipoVenta(rs.getString("TipoVenta")) + ";");
				bw.write(tipoNumeroDistribuicion(rs.getString("N_OrdenDistribu")) + ";");
				bw.write(horario((rs.getString("hora"))) + ";");
				bw.write(((rs.getString("hora"))) + ";");
				bw.write(((rs.getString("fecha_creacion"))) + "\n");
				
				
				
				
				
				
				pstmtVentataVerde.setString(1, ( rs.getString("tc_po_line_id") != null?  rs.getString("tc_po_line_id") : "-" ));
				pstmtVentataVerde.setString(2, rs.getString("N_SolicitudCliente"));
				pstmtVentataVerde.setString(3, rs.getString("N_OrdenDistribu"));
				pstmtVentataVerde.setString(4, rs.getString("Fecha_Creacion_Orden"));
				pstmtVentataVerde.setString(5, rs.getString("FechaCompromiso"));
				pstmtVentataVerde.setString(6, rs.getString("EstOrden"));
				pstmtVentataVerde.setString(7, rs.getString("EstLineaOrden"));
				pstmtVentataVerde.setString(8, rs.getString("Estado_OD"));
				pstmtVentataVerde.setString(9, rs.getString("SKU"));
				pstmtVentataVerde.setString(10, rs.getString("DescSKU"));
				pstmtVentataVerde.setString(11, rs.getString("CantSKU"));
				pstmtVentataVerde.setString(12, rs.getString("DESPACHO"));
				pstmtVentataVerde.setString(13, rs.getString("Tipo_Orden"));
				pstmtVentataVerde.setString(14, rs.getString("Mes"));
				pstmtVentataVerde.setString(15, rs.getString("Ano"));
				pstmtVentataVerde.setString(16, rs.getString("TipoVenta"));
				pstmtVentataVerde.setInt(17, tipoVenta(rs.getString("TipoVenta")));
				pstmtVentataVerde.setInt(18, tipoNumeroDistribuicion(rs.getString("N_OrdenDistribu")));
				pstmtVentataVerde.setInt(19, horario((rs.getString("hora"))));
				pstmtVentataVerde.setInt(20, ((rs.getInt("hora"))));
				pstmtVentataVerde.setString(21, rs.getString("fecha_creacion"));
				pstmtVentataVerde.executeUpdate();
				commit(dbconnOracle2,"COMMIT");
				
				
				/*
				if(!ejecutarPaso6(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle)){

					
					bw.write(rs.getString("OD_EOM")+";");
					bw.write(rs.getString("NUMERO_SD")+";");
					bw.write(rs.getString("FECHA")+";");
					bw.write("0"+";");
					bw.write(rs.getString("NUMTERTSL")+";");
					bw.write(rs.getString("NUMTRANTSL")+";");
					bw.write("0"+";");
					bw.write(rs.getString("SKU")+";");
					bw.write("0"+";");
					bw.write("0"+";");
					bw.write(ejecutarPaso7(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle)+";");
					bw.write("0"+";");
					bw.write("0"+";");
					bw.write("0"+";");
					bw.write("5"+"\n");
					
					
					pstmt2.setString(1, rs.getString("OD_EOM"));
					pstmt2.setInt(2, rs.getInt("NUMERO_SD"));
					pstmt2.setString(3, rs.getString("FECHA"));
					pstmt2.setInt(4, 0);
					pstmt2.setInt(5, rs.getInt("NUMTERTSL"));
					pstmt2.setInt(6, rs.getInt("NUMTRANTSL"));
					pstmt2.setInt(7, 0);
					pstmt2.setInt(8, rs.getInt("SKU"));
					pstmt2.setInt(9, 0);
					pstmt2.setInt(10, 0);
					pstmt2.setInt(11, ejecutarPaso7(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle));
					pstmt2.setInt(12, 0);
					pstmt2.setInt(13, 0);
					pstmt2.setInt(14, 0);
					pstmt2.setInt(15, 5);
					pstmt2.executeUpdate();
					commit(dbconnOracle2,"COMMIT");
					
				} else if(ejecutarPaso6(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle)){
					
					info("ejecutarPaso7 Encontrado ="+ejecutarPaso7(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle));
					
					bw.write(rs.getString("OD_EOM")+";");
					bw.write(rs.getString("NUMERO_SD")+";");
					bw.write(rs.getString("FECHA")+";");
					bw.write("0"+";");
					bw.write(rs.getString("NUMTERTSL")+";");
					bw.write(rs.getString("NUMTRANTSL")+";");
					bw.write("0"+";");
					bw.write(rs.getString("SKU")+";");
					bw.write("0"+";");
					bw.write("0"+";");
					bw.write(ejecutarPaso7(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle)+";");
					bw.write("0"+";");
					bw.write("0"+";");
					bw.write("1"+";");
					bw.write("5"+"\n");
					
					
					pstmt2.setString(1, rs.getString("OD_EOM"));
					pstmt2.setInt(2, rs.getInt("NUMERO_SD"));
					pstmt2.setString(3, rs.getString("FECHA"));
					pstmt2.setInt(4, 0);
					pstmt2.setInt(5, rs.getInt("NUMTERTSL"));
					pstmt2.setInt(6, rs.getInt("NUMTRANTSL"));
					pstmt2.setInt(7, 0);
					pstmt2.setInt(8, rs.getInt("SKU"));
					pstmt2.setInt(9, 0);
					pstmt2.setInt(10, 0);
					pstmt2.setInt(11, ejecutarPaso7(fechaS, loloca, numTer, numTran, pxq, sku, odEom, dbconnOracle));
					pstmt2.setInt(12, 0);
					pstmt2.setInt(13, 0);
					pstmt2.setInt(14, 1);
					pstmt2.setInt(15, 5);
					pstmt2.executeUpdate();
					commit(dbconnOracle2,"COMMIT");
				}
				*/
			}
			bw.write(sb.toString());
			
			///###########################	CARGAR TABLA CUADRATURA_JPDTOTDAD	###########################
						


			info("Archivos creados.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnection,null,bw);
		}
		
	}
	
	/**
	 * Metodo que hace commit en la base de datos
	 * 
	 * @param Connection, conexion a la base de datos
	 * @return si valor de retorno
	*/
	
	private static void commit(Connection dbconnection,  String sql) {
		PreparedStatement pstmt = null;
		try {
			pstmt = dbconnection.prepareStatement(sql);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			cerrarTodo(null, pstmt, null);
		}
	}
	
	
	/**
	 * Metodo que ejecuta la eliminacion de registros en una tabla 
	 * 
	 * @param Connection, conexion de las base de datos
	 * @param String, query para la eliminacion  
	 * @return 
	 */
	private static void elimnarCuadratura(Connection dbconnection,  String sql) {
		info("[sql Eliminar] " + sql);
		PreparedStatement pstmt = null;
		try {
			pstmt = dbconnection.prepareStatement(sql);
			System.out.println("registros elimnados Cuadraturas : " + pstmt.executeUpdate());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			cerrarTodo(null, pstmt, null);
		}
		
	}
	
	

	

	
	/**
	 * Metodo que elimina un archivo
	 * 
	 * @param String, ruta especifica del archivo
	 * @return
	 */
	public static void eliminarArchivo(String archivo) {

		File fichero = new File(archivo);

		if (fichero.delete()) {

			System.out.println("archivo eliminado");

		} else {
			System.out.println("archivo no eliminado");
		}

	}

	/*
	 * private static String ejecutarQueryDiario(String Citaan, String total,
	 * Connection dbconnection, PreparedStatement pstmtCuadratura) {
	 * 
	 * StringBuffer sb = new StringBuffer(); PreparedStatement pstmt = null;
	 * System.out.println("Citaan="+Citaan);
	 * System.out.println("Citaan2="+Citaan.replace("T", ""));
	 * 
	 * try {
	 * 
	 * sb = new StringBuffer(); //sb.
	 * append("Select CASE WHEN E.Citaan IS NOT NULL THEN E.Citaan ELSE '0' END  AS ASN, SUM(P.Pounts) AS Cantidad  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619 AND E.Citaan='"
	 * +Citaan+"') GROUP BY E.Citaan"); sb.
	 * append("Select E.Citaan  AS ASN, SUM(P.Pounts) AS Cantidad  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619 AND E.Citaan='"
	 * +Citaan.replace("T", "").replace("C", "").replace("D",
	 * "")+"') GROUP BY E.Citaan");
	 * 
	 * //sb.
	 * append("SELECT Invaud.Itrloc, Invaud.INUMBR, Invaud.ITRTYP, Invaud.ITRDAT, Invaud.ITPDTE, Invaud.IDEPT, Invaud.ITRREF, Invaud.ITRAF1 FROM mmsp4lib.Invaud Invaud WHERE (Invaud.Itrtyp=31 AND Invaud.Itrdat>170101 AND Invaud.Itrloc=12)"
	 * ); pstmt = dbconnection.prepareStatement(sb.toString()); ResultSet rs =
	 * pstmt.executeQuery(); sb = new StringBuffer();
	 * 
	 * boolean reg = false; do{ reg = rs.next(); if (reg){
	 * sb.append(rs.getString("ASN") + ";"); sb.append(rs.getString("Cantidad")
	 * + ";");
	 * sb.append(String.valueOf((Integer.parseInt(rs.getString("Cantidad")) -
	 * Integer.parseInt(total))) + ";");
	 * sb.append(obtenerEstado(rs.getString("ASN") ,
	 * (Integer.parseInt(rs.getString("Cantidad")) - Integer.parseInt(total))) +
	 * "\n"); break; }else{ //sb.append("\n");
	 * 
	 * sb.append("#N/A" + ";"); sb.append("#N/A" + ";"); sb.append("#N/A" +
	 * ";"); sb.append("#N/A" + "\n"); } } while (reg); } catch (Exception e) {
	 * 
	 * e.printStackTrace(); info("[crearTxt2]Exception:"+e.getMessage()); }
	 * finally {
	 * 
	 * cerrarTodo(null,pstmt,null); } return sb.toString(); }
	 */

	/**
	 * Metodo que crea la conexion a la base de datos ROBLE JDA 
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @return 
	 * 
	 */
	private static Connection crearConexion() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a ProducciÃ³n. por el momento
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}
	
	/**
	 * Metodo que crea la conexion a la base de datos a KPI
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @return 
	 * 
	 */
	private static Connection crearConexionOracle2() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@172.18.163.15:1521/XE", "kpiweb","kpiweb");
		} catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}
	
	/**
	 * Metodo que crea la conexion a la base de datos a MEOMCLP
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @return 
	 * 
	 
	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a ProducciÃ³n. por el momento
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}*/ 

	
	
	
	/**
	 * Metodo que cierra la conexion, Procedimintos,  BufferedWriter
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param PreparedStatement, Objeto que representa una instrucción SQL precompilada. 
	 * @return retorna
	 * 
	 */
	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw) {

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		} catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:" + e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		} catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:" + e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		} catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:" + e.getMessage());
		}
	}

	/**
	 * Metodo que muestra informacion 
	 * 
	 * @param String, texto a mostra
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static void info(String texto) {

		try {

			bw.write(texto + "\n");
			bw.flush();
		} catch (Exception e) {

			System.out.println("Exception:" + e.getMessage());
		}
	}

	/**
	 * Metodo que resta dias segun dia ingresado
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}

	/**
	 * Metodo que da un formato de fecha
	 * 
	 * @param String, fecha para formatear  
	 * @return 
	 */
	public static String getFormatDay(String fecha) {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		// String dateInString = "01/11/2014";

		try {

			Date date = formatter.parse(fecha);
			// System.out.println(date);
			// System.out.println(formatter.format(date));
			return formatter.format(date);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;

	}
	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	private static int tipoVenta(String iOptTipoVenta) {

		int result = 0;
		int sOutPut = 0;

		try {
			switch (iOptTipoVenta) {

			case "VeV PD":
				sOutPut = 1;
				break;
			case "Stock CD":
				sOutPut = 2;
				break;
			case "VeV CD":
				sOutPut = 3;
				break;
			} 
			
			if (iOptTipoVenta == "" && iOptTipoVenta != null) {
				result = 0;
			} else {
				result = sOutPut;
			}
		}
		catch (Exception e) {

			info("[result]Exception 2:"+e.getMessage());
		}
		return result;
	}
	
	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	private static int tipoNumeroDistribuicion(String iOptNumeroDictribuicio) {
		info("iOptNumeroDictribuicio:"+iOptNumeroDictribuicio);
		int result = 0;
		//int sOutPut = 0;
		
		if (iOptNumeroDictribuicio !=null && iOptNumeroDictribuicio !="" && iOptNumeroDictribuicio !="0") {
			result = 2;
		} else {
			result = 1;
		}
		
		/*
		try {
			switch (iOptNumeroDictribuicio) {

			case "0":
				sOutPut = 0;
				break;
			case "null":
				sOutPut = 0;
				break;	
			default:
				sOutPut = 4;
			} 
			
			
		}
		catch (Exception e) {

			info("[result]Exception 2:"+e.getMessage());
		}
		*/
		return result;
	}
	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	private static int horario(String iOptFechaCreacion) {
		info("iOptFechaCreacion:"+iOptFechaCreacion);
		//27-04-2017 22:05:34
		
		
		int result = 0;
		//int sOutPut = 0;
		
		if (iOptFechaCreacion !=null && iOptFechaCreacion !="") {
			//info("iOptFechaCreacion:"+iOptFechaCreacion.substring(4,6));
			result = 2;
		} else {
			result = 1;
		}
		
		/*
		try {
			switch (iOptNumeroDictribuicio) {

			case "0":
				sOutPut = 0;
				break;
			case "null":
				sOutPut = 0;
				break;	
			default:
				sOutPut = 4;
			} 
			
			
		}
		catch (Exception e) {

			info("[result]Exception 2:"+e.getMessage());
		}
		*/
		return result;
	}
	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 * 
	 
	private static String formatDate(Date fecha, int iOptFormat) {

		String sFormatedDate = null;
		String sFormat = null;

		try {

			SimpleDateFormat df = null;

			switch (iOptFormat) {

			case 0:
				sFormat = "dd/MM/yy HH:mm:ss,SSS";
				break;
			case 1:
				sFormat = "dd/MM/yy";
				break;
			case 2:
				sFormat = "dd/MM/yy HH:mm:ss";
				break;
			case 3:
				sFormat = "yyyy-MM-dd HH:mm:ss,SSS";
				break;
			}
			df = new SimpleDateFormat(sFormat);
			sFormatedDate = df.format(fecha != null ? fecha:new Date(0));

			if (iOptFormat == 0 && sFormatedDate != null) {

				sFormatedDate = sFormatedDate + "000000";
			}
		}
		catch (Exception e) {

			info("[formatDate]Exception:"+e.getMessage());
		}
		return sFormatedDate;
	}
	*/

}
