package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private class Node{
		String id;
		String name;
	}

	private class idcomparator implements Comparator<Node>
	{
		public int compare(Node n1, Node n2)
		{
			return n1.id.compareTo(n2.id);
		}
	}

	private boolean insertflag = true;

	private int recoverwait = 0;

	private Cursor MC;
	private boolean MCflag = false;

	private HashMap<String,String> insertwait = new HashMap<String, String>();

	private HashMap<String,String> hmflag = new HashMap<String, String>();

	private ArrayList<String>[] recovery = new ArrayList[5];

	private int myindex;

	private List<Node> emulist = new ArrayList<Node>(5);

	private DBHelper myDbHelper;

	private static final String dbname = "db";

	private SQLiteDatabase db;

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	static final String[] remote_ports = {"11108","11112","11116","11120","11124"};
	static final int SERVER_PORT = 10000;


	@Override
	public boolean onCreate() {

		myDbHelper = new DBHelper(getContext(),dbname);

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myport = String.valueOf((Integer.parseInt(portStr) * 2));

		try {

			for (int i = 0; i < 5; i++) {

				recovery[i] = new ArrayList<String>();

				Node temp = new Node();

				temp.name = remote_ports[i];
				temp.id = genHash(String.valueOf(Integer.parseInt(remote_ports[i]) / 2));

				emulist.add(temp);

			}

			Collections.sort(emulist, new idcomparator());

			for (int i = 0; i < 5; i++) {
				if (emulist.get(i).name.equals(myport)){
					myindex = i;
				}
			}

			Log.e(TAG,"myindex = " + myindex);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recover");

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			//while (recoverwait!=4) {}
			Thread.sleep(1000);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();

		} catch (IOException e) {
			//Log.e(TAG, "Can't create a ServerSocket");
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return true;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		db = myDbHelper.getReadableDatabase();

		if(selection.equals("*"))
			db.execSQL("DELETE FROM "+dbname);

		else if(selection.equals("@"))
			db.execSQL("DELETE FROM "+dbname);

		/*else {
			Cursor cr;
			do {
				db.execSQL("DELETE FROM " + dbname + " WHERE key='" + selection + "'");
				cr = db.rawQuery("SELECT changes() AS affected_row_count", null);
			} while (cr.getCount() == 0);
		}*/
		else {
			db.execSQL("DELETE FROM " + dbname + " WHERE key='" + selection + "'");

			int index = getpartitionindex(selection);
			if (index != myindex) {

				String msgtosend = "delete-" + Integer.toString(index) + "-" + selection;

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
				}
			}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String key = (String) values.get("key");
		String value = (String) values.get("value");

		while(insertwait.containsKey(key)) {}

		int targetindex = getpartitionindex(key);

		//Log.e(TAG,"key = "+key+" value = "+value+" index = "+targetindex);

		if (targetindex == myindex || (targetindex+1)%5 == myindex || (targetindex+2)%5 == myindex) {

			insertvals(key,value,targetindex);

		}

		String msgtosend = "insert-" + targetindex + '-' + key + '-' + value;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);

		while(!insertwait.containsKey(key)) {}

		insertwait.remove(key);

		//Log.e(TAG,"Exiting insert");

		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		db = myDbHelper.getReadableDatabase();

		if (selection.equals("*")) {

			while (MCflag) {}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "selectall");

			while (!MCflag) {}

			MCflag = false;

			return MC;
		}
		else if (selection.equals("@")) {

			return db.rawQuery("SELECT key,value FROM "+dbname,null);
		}
		else {
			int index = getpartitionindex(selection);

			Cursor cr;
			if(index == myindex || (index+1)%5 == myindex || (index+2)%5 == myindex) {
				do {
					/*try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}*/
					cr = db.rawQuery("SELECT key,value FROM " + dbname + " WHERE key='" + selection + "'", null);
				}while (cr.getCount()==0);
				return cr;
			}
			else {

				String msgtosend = "query-"+Integer.toString(index)+"-"+selection;

				//while (MCflag) {}

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);

				while (!hmflag.containsKey(selection)) {}

				MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
				cursor.addRow(new String[]{selection,hmflag.get(selection)});
				hmflag.remove(selection);
				return cursor;
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];

			try {
				while (true) {

					//Log.e(TAG,"SERVER waiting");

					Socket clientSocket = serverSocket.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

					String message;
					if((message = in.readLine()) != null) {

						//Log.e(TAG,"SERVER : msg recieved "+message);
						String line[] = message.split("-");

						if (line[0].equals("recovery")){

							clientSocket.close();
							int index = Integer.parseInt(line[1]);
							String msgtosend = "";

							if(!recovery[index].isEmpty())
								msgtosend = TextUtils.join(":",recovery[index].toArray());

							Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(index).name));
							PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);

							out1.println("recovered-"+msgtosend);
							out1.flush();
						}

						else if (line[0].equals("recovered")){

							clientSocket.close();
							if (line.length>1) {
								String vals[] = line[1].split(":");
								for (String ele : vals){
									String kv[] = ele.split("@");
									insertvals(kv[0],kv[1],Integer.parseInt(kv[2]));
								}
							}
							//recoverwait++;
						}

						else if (line[0].equals("insert")) {

							String key = line[2];
							String value = line[3];
							int index = Integer.parseInt(line[1]);

							insertvals(key,value,index);

							//Log.e(TAG,"SERVER inserted");

							out.println("Ack");
							out.flush();
						}

						else if (line[0].equals("query")){

							out.println(querykey(line[1]));
							out.flush();
						}

						else if (line[0].equals("selectall")){

							out.println(queryall(line[1]));
							out.flush();
						}

						else if (line[0].equals("delete")){

							db = myDbHelper.getReadableDatabase();
							db.execSQL("DELETE FROM " + dbname + " WHERE key='" + line[1] + "'");
							clientSocket.close();
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			//Log.e(TAG,"CLIENT - to send "+msgs[0]);

			String line[] = msgs[0].split("-");

			//String msgtosend = msgs[0];
			int index = -1;

			if (line.length > 1)
				index = Integer.parseInt(line[1]);

			try {

				if (line[0].equals("recover")) {
					for (int i=0;i<5;i++) {
						if(myindex != i) {

							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(i).name));
							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

							out.println("recovery-"+myindex);
							out.flush();

						}
					}
				}
				else if (line[0].equals("insert")) {

					String msgtosend = "insert-" + line[1] + "-" + line[2] + "-" + line[3];

					if (index != myindex) {

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(index).name));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						//Log.e(TAG,"CLIENT - Socket created");
						out.println(msgtosend);
						out.flush();

						String input;
						if ((input = in.readLine()) != null) {
							if (input.equals("Ack")) {
								//Log.e(TAG,"CLIENT - Ack recieved for"+msgtosend);
								in.close();
								socket.close();
							}
						}
						else {
							recovery[index].add(line[2] + "@" + line[3] + "@" + line[1]);
						}
					}

					//Log.e(TAG,"CLIENT Sending first dup");
					int indextosend = (index+1)%5;
					String input;

					if (indextosend != myindex) {
						Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(indextosend).name));
						PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);
						BufferedReader in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));

						//Log.e(TAG, "CLIENT - Socket created");
						out2.println(msgtosend);
						out2.flush();

						if ((input = in2.readLine()) != null) {
							if (input.equals("Ack")) {
								//Log.e(TAG, "CLIENT - DUP1 Ack recieved for" + msgtosend);
								in2.close();
								socket2.close();
							}
						} else {
							recovery[indextosend].add(line[2] + "@" + line[3] + "@" + line[1]);
						}
					}

					//Log.e(TAG,"Sending second dup");

					indextosend = (index+2)%5;

					if (indextosend != myindex) {
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(indextosend).name));
						PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
						BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));

						//Log.e(TAG, "CLIENT - Socket created");
						out1.println(msgtosend);
						out1.flush();

						if ((input = in1.readLine()) != null) {
							if (input.equals("Ack")) {
								//Log.e(TAG, "CLIENT - DUP2 Ack recieved for" + msgtosend);
								in1.close();
								socket1.close();
							}
						} else {
							recovery[indextosend].add(line[2] + "@" + line[3] + "@" + line[1]);
						}
					}

					insertwait.put(line[2],line[3]);
				}
				else if (line[0].equals("selectall")){

					db = myDbHelper.getReadableDatabase();

					Cursor cr[] = new Cursor[5];

					for (int i=0; i<5; i++) {
						//Log.e(TAG,"index="+i);
						if (i == myindex || (i+1)%5 == myindex || (i+2)%5 == myindex) {

							cr[i] = db.rawQuery("SELECT key,value FROM " + dbname + " WHERE partition="+Integer.toString(i), null);
						}

						else {

							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(i).name));
							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

							//Log.e(TAG,"CLIENT - Socket created");
							out.println("selectall-"+i);
							out.flush();

							String input;
							if ((input = in.readLine()) != null) {
								//Log.e(TAG,"input = "+input);

								if(!input.equals("")) {
									String result[] = input.split("-");

									MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});

									for (String ele : result) {
										String kv[] = ele.split("@");

										mc.addRow(new String[]{kv[0], kv[1]});
									}

									cr[i] = mc;
								}
								else {
									//Log.e(TAG,"NULL");
									cr[i] = null;
								}
							}
							else {
								Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get((i+1)%5).name));
								PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
								BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));

								//Log.e(TAG,"CLIENT - Socket created");
								out1.println("selectall-"+i);
								out1.flush();

								if ((input = in1.readLine()) != null) {
									//Log.e(TAG,"input = "+input);

									if (!input.equals("")) {
										String result[] = input.split("-");

										MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});

										for (String ele : result) {
											String kv[] = ele.split("@");

											mc.addRow(new String[]{kv[0], kv[1]});
										}

										cr[i] = mc;
									} else {
										//Log.e(TAG, "NULL");
										cr[i] = null;
									}
								}
							}
							in.close();
							socket.close();
						}
					}

					MC = new MergeCursor(cr);

					MCflag = true;
				}
				else if (line[0].equals("query")){

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(Integer.valueOf(line[1])).name));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					//Log.e(TAG,"CLIENT - Socket created");
					out.println("query-"+line[2]);
					out.flush();

					String value;
					if ((value = in.readLine()) != null) {
						//Log.e(TAG,"query result from i "+value);
						//MC = cursor;

						hmflag.put(line[2],value);
					}
					else {
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get((Integer.valueOf(line[1])+1)%5).name));
						PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
						BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));

						//Log.e(TAG,"CLIENT - Socket created");
						out1.println("query-"+line[2]);
						out1.flush();

						if ((value = in1.readLine()) != null) {
							//Log.e(TAG,"query result from dup "+value);
							//MC = cursor;

							hmflag.put(line[2],value);
						}
						in1.close();
						socket1.close();
					}

					in.close();
					socket.close();
				}
				else if (line[0].equals("delete")){

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(Integer.valueOf(line[1])).name));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println("delete-"+line[2]);
					out.flush();
				}

		} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	/*private boolean sendmsg (String msg, int indextosend){

		Log.e(TAG,"sendmsg - "+msg);

		try {
			//Log.e(TAG,"CLIENT - Creating socket");
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(emulist.get(indextosend).name));
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			//Log.e(TAG,"CLIENT - Socket created");
			out.println(msg);
			out.flush();

			String input;
			if ((input = in.readLine()) != null) {
				if (input.equals("Ack")) {
					Log.e(TAG,"CLIENT - Ack recieved for"+msg);
					in.close();
					socket.close();
					return true;
				}
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
			Log.e(TAG,e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			Log.e(TAG,e.getMessage());
		}

		return false;
	}*/

	private int getpartitionindex (String msg){
		try {

			String hash = genHash(msg);
			for (int i=0;i<5;i++){
				if(hash.compareTo(emulist.get(i).id)<0){
					return i;
				}
			}
			return 0;

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public void insertvals(String key, String value, int index){

		while(!insertflag) {}

		insertflag = false;

		db = myDbHelper.getWritableDatabase();

		ContentValues values = new ContentValues();
		int version = queryversion(key)+1;

		values.put("key",key);
		values.put("value",value);
		values.put("version",version);
		values.put("partition",index);

		if ( version == 1) {

			db.insert(dbname,"value",values);
			//Log.e(TAG,"INSERTED "+key);
		}
		else {

			db.update(dbname,values,"key='"+key+"'",null);
			//Log.e(TAG,"UPDATED "+key);
		}

		insertflag = true;
	}

	public String queryall(String index) {

		db = myDbHelper.getWritableDatabase();

		Cursor cr = db.rawQuery("SELECT * FROM " + dbname +" WHERE partition = "+index, null);

		cr.moveToFirst();

		String msg = "";

		if (cr.getCount()>0) {

			int ik = cr.getColumnIndexOrThrow("key");
			int iv = cr.getColumnIndexOrThrow("value");

			msg = cr.getString(ik) + "@" + cr.getString(iv);

			while (cr.moveToNext()) {

				msg += "-" + cr.getString(ik) + "@" + cr.getString(iv);
			}
		}
		return msg;
	}

	public String querykey(String key){

		db = myDbHelper.getWritableDatabase();

		Cursor cr;
		do {
			/*try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
			cr = db.rawQuery("SELECT key,value FROM " + dbname + " WHERE key='" + key + "'", null);
		} while(cr.getCount()==0);

		cr.moveToFirst();
		String value = cr.getString(cr.getColumnIndexOrThrow("value"));
		cr.close();
		return value;
	}

	public int queryversion(String key){

		db = myDbHelper.getWritableDatabase();

		Cursor cr;
		cr = db.rawQuery("SELECT * FROM "+dbname+" WHERE key='"+key+"'",null);
		cr.moveToFirst();

		if (cr.getCount()==0){

			return 0;
		}
		else{

			int version = cr.getInt(cr.getColumnIndexOrThrow("version"));
			cr.close();
			return version;
		}
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
