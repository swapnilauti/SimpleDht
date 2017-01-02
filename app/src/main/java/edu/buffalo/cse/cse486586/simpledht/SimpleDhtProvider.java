/*
* Name : Swapnil Sudam Auti
* CSE 586
* person # 50169614
* email - sauti@buffalo.edu
* */
package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {
    private String myPort = null;
    private String myNodeId = null;
    private String portStr = null;
    private String predecessorNodeId = null;
    private String predecessorPort = null;
    private String successorNodeId = null;
    private String successorPort = null;
    private HashMap<String,Socket> socketHM;
    private HashSet<Integer> remotePorts;
    private TreeMap<String,String> nodeTree;
    private Socket successorNodeSocket = null;
    private Uri mUri = null;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    ServerSocket serverSocket = null;
    String hmString = null;
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
         /*
        * 1. if selection.equals("*") -> delete all local values
        *       if(selectionArgs==null)
        *           append this node id to selectionArgs and send same request to successor nodes
        *       else if(selectionArgs[0]!=mySuccessor)
        *           send same request to successor nodes
        *
        * 2. else if selection.equals("@") -> delete all local values
        * 3. else if hash(key) is in local node range -> delete local <key,value>
        * 4. else send request to successor node
        * */
        String key = selection;
        if(getContext().deleteFile(key))
            return 1;

        /*
        * VERY IMPORTANT TO RETURN ALL FILES
        * */
        if(selection.equals("*") || selection.equals("@")) {
            String fileNames[] = getContext().fileList();
            for(int i=0;i<fileNames.length;i++) {
                key = fileNames[i];
                try {
                    getContext().deleteFile(key);
                } catch (Exception e) {
                    Log.e("delete", "Error : delete -> error deleting all files");
                }
            }
        }
        if(selection.equals("*")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "3", key);
        }
        else if(!selection.equals("*") && !selection.equals("@")){
            String keyHash = null;
            try{
                keyHash = genHash(key);
            }catch (NoSuchAlgorithmException e){
                Log.e("delete ", "Error : no such algo exception");
            }
            if(isThisNode(keyHash)) {
                    /*
                        Key belong to this node
                     */
                try {

                    getContext().deleteFile(key);
                }catch (Exception e)
                {
                    Log.e("delete", "Error : delete -> error while deleting file");
                }
            }
            else{
                /*
                    key belongs to other node
                 */
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "3", key);
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
        * 1. check hash(key),
        *       if for this node, insert
        *       else send to successor by creating new ClientTask

        * */
        FileOutputStream f= null;
        String key = values.getAsString("key");
        Log.d("insert", "Key  : "+key);
        String value = values.getAsString("value");
        Log.d("insert", "value : "+value);
        String keyHash = null;
        Log.d("insert", "key " + key + " value" + value);
        try{
            keyHash = genHash(key);
        }catch (NoSuchAlgorithmException e){
            Log.e("query", "Error : no such algo exception");
        }
        if(isThisNode(keyHash)) {
            Log.e("insert", "key belongs to this node");
            try {
                f = getContext().openFileOutput(key, 0);
            } catch (FileNotFoundException e) {
                Log.e("insert", "Error : insert-> File not found");
            }
            try {
                f.write(value.getBytes());
                Log.d("insert", "Insertion done key = "+key+" keyHash= "+keyHash);
            } catch (IOException e) {
                Log.e("insert", "Error : insert-> IO Exception");
            }
            finally {
                try {
                    f.close();
                }catch (Exception e){
                    Log.e("insert", "Error : insert-> Can't close file");
                }
            }
        }
        else{
            Log.d("insert", "key does not belongs to this node");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "2", key, value);
        }

        Log.d("insert", values.toString());
        return uri;
    }
    private void initHashSet(){
        remotePorts = new HashSet<Integer>();
        remotePorts.add(11108);
        remotePorts.add(11112);
        remotePorts.add(11116);
        remotePorts.add(11120);
        remotePorts.add(11124);
    }


    private void updatePreSuc(){
        Map.Entry<String,String> mapEntry = null;
        mapEntry = nodeTree.higherEntry(myNodeId);
        if(mapEntry==null){
            mapEntry = nodeTree.firstEntry();
            
        }
        successorNodeId = mapEntry.getKey();
        Integer temp = Integer.parseInt(mapEntry.getValue())*2;
        successorPort = temp.toString();
        mapEntry = nodeTree.lowerEntry(myNodeId);
        if(mapEntry==null){
            mapEntry = nodeTree.lastEntry();

        }
        predecessorNodeId = mapEntry.getKey();
        predecessorPort = mapEntry.getValue();
        Log.d("updatePreSuc", nodeTree.toString());
    }
    private boolean isThisNode(String keyHash){
        /*
            1. if only one node is present
            2. common case (Handled)
            3. if this is first node
         */
        if(nodeTree.firstKey().equals(nodeTree.lastKey())){
            return true;
        }
        if(keyHash.compareTo(predecessorNodeId)>0 && keyHash.compareTo(myNodeId) <= 0){
            return true;
        }
        if(nodeTree.firstEntry().getKey().equals(myNodeId) && (keyHash.compareTo(nodeTree.lastKey())>0 || keyHash.compareTo(myNodeId)<=0))
            return true;

        return false;
    }

    private MatrixCursor stringToCursor(MatrixCursor m, String hmString){
        if(hmString==null || hmString.equals("")) {
            Log.d("stringToCursor", "hmString is null");
            return m;
        }
        else {
            Log.d("stringToCursor", "hmString before removing {} "+hmString);
            hmString = hmString.substring(1, hmString.length() - 1);

            String entries[] = hmString.split(",");
            Log.d("stringToCursor", "hmString size "+entries.length);
            for (int i = 0; i < entries.length; i++) {
                String temp[] = entries[i].split("=");
                String[] row = new String[2];
                row[0] = temp[0].trim();
                row[1] = temp[1].trim();
                m.addRow(row);
            }
        }
        return m;
    }
    @Override
    public boolean onCreate() {

        initHashSet();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        try {
            myNodeId = genHash(portStr);
        }catch (NoSuchAlgorithmException e){
            Log.e("onCreate", "no Such algorithm Exception");
        }
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        socketHM = new HashMap<String, Socket>();
        nodeTree = new TreeMap<String, String>();

        nodeTree.put(myNodeId,portStr);
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        try {
            serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("onCreate", "Can't create a ServerSocket");

        }
        predecessorNodeId = successorNodeId = myNodeId;
        Log.d("onCreate", "Calling Client Task");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "0");

        Log.d("onCreate", "my Node Id "+ myNodeId);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs,String sortOrder) {
        /*

        * 1. if selection.equals("*") -> return global Cursor
        * else if selection.equals("@") -> return local Cursor
        * else if hash(key) is in local node range -> return local <key,value> cursor
        * else send request to successor node
        * */
        Log.d("query", selection);
        FileInputStream f= null;
        BufferedReader br = null;
        String key = selection;
        String columnNm[] = {"key","value"};
        String columnVal[]= new String[2];
        MatrixCursor m = new MatrixCursor(columnNm);
        /*
        * VERY IMPORTANT TO RETURN ALL FILES
        * */
        Log.d("query", "selection value = "+selection);
        if(selection.equals("*") || selection.equals("@")) {
            Log.d("query", "inside selection.equals(\"*\") || selection.equals(\"@\")");
            String fileNames[] = getContext().fileList();
            for(int i=0;i<fileNames.length;i++) {
                key = fileNames[i];
                Log.d("query", "file name  = " + key);
                try {
                    f = getContext().openFileInput(key);
                } catch (FileNotFoundException e) {
                    Log.d("query", "Error : Selection = \"*\" -> opening file input ");
                }
                try{
                    br = new BufferedReader(new InputStreamReader(f));
                    columnVal[0]=key;
                    columnVal[1]=br.readLine();
                    m.addRow(columnVal);
                }
                catch(IOException e)
                {
                    Log.e("query", "Error : Selection = \"*\" & \"@\"-> creating bufferedReader ");
                }
                finally {
                    try {
                        f.close();
                        br.close();
                        Log.d("query", "Error : Size of matrix after initial insertion " + m.getCount());
                    }catch (IOException e){
                        Log.e("query", "Error : Selection = \"*\" & \"@\" -> closing f and br ");
                    }
                }
            }
        }
        if(selection.equals("*")){
            Log.d("query", "selection.equals(\"*\")");
            ClientTask ct = new ClientTask();
            if(projection==null)
                ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"1",selection,myPort);
            else
                ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"1",selection,projection[0]);
            try {
                ct.get();
            }catch (Exception e){
                Log.e("query", "Error : Selection = \"*\" -> timeout exception ");
            }
            m=stringToCursor(m,hmString);
            hmString = null;
        }
        else if(!selection.equals("@") && !(selection.equals("*"))){

            String keyHash = null;
            try{
                keyHash = genHash(key);
            }catch (NoSuchAlgorithmException e){
                Log.e("query", "Error : no such algo exception");
            }
            if(isThisNode(keyHash)) {
                Log.d("Query", "if this is a simple key & belongs to this node");
                    /*
                        Key belong to this node
                     */
                try {
                    f = getContext().openFileInput(selection);
                }catch (FileNotFoundException e)
                {
                    Log.e("insert", "Error : File not found in this node where the file is supposed to be present");
                }

                try{
                    br = new BufferedReader(new InputStreamReader(f));
                    columnVal[0]=key;
                    columnVal[1]=br.readLine();
                    m.addRow(columnVal);
                }
                catch(IOException e)
                {
                    Log.e("insert", "Error : GroupMessengerProvider.java-> query -> IO Exception");
                }
                finally {
                    try {
                        f.close();
                        br.close();
                    }catch (IOException e){
                        Log.e("query", "Error : f close IOException ");
                    }
                }


            }
            else{
                /*
                    key belongs to other node
                 */
                Log.d("query", "if this is a simple key & belongs to other node ");
                ClientTask ct = new ClientTask();
                ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"1",key,myPort);
                try {
                    ct.get();
                }catch (Exception e){
                    Log.e("query", "Error : Selection = \"*\" -> timeout exception ");
                }
                m=stringToCursor(m,hmString);
                hmString = null;

            }
        }
        return m;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        if(input == null)
            return null;
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String cursorToString(MatrixCursor m){
        if(m == null||m.getCount()==0)
            return "";
        String key = null;
        String value = null;
        HashMap<String,String> hm = new HashMap<String, String>();
        m.moveToFirst();
        while(!m.isAfterLast()){
            key = m.getString(0);
            value = m.getString(1);
            hm.put(key,value);
            m.moveToNext();
        }
        return hm.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader br = null;
            PrintWriter out = null;
            OutputStream os = null;
            InputStream is = null;
            Socket socket = null;
            MatrixCursor m =null;
            ContentValues cv = null;
            while (true) {
                try {
                    socket = serverSocket.accept();
                    is = socket.getInputStream();
                    br = new BufferedReader(new InputStreamReader(is));
                    String temp = br.readLine();
                    if (temp != null && temp != "") {
                        String arr[] = temp.split(" ");
                        String socketHash = null;
                        switch (Integer.parseInt(arr[0])) {
                            case 0:
                                try {
                                    socketHash = genHash(arr[1]);
                                }catch (Exception e){
                                    Log.e("Server Task", "Exception while generating hash for successor node ");
                                }
                                try {
                                    os = socket.getOutputStream();
                                    out = new PrintWriter(os, true);
                                    out.println("Yes");
                                    out.flush();
                                }finally{
                                    out.close();
                                    os.close();
                                }
                                nodeTree.put(socketHash,arr[1]);
                                updatePreSuc();
                                break;
                            case 1:
                                if(arr[1].equals("*")){
                                    if(arr[2].equals(myPort))
                                        m = null;
                                    else
                                        m = (MatrixCursor) query(mUri, new String[]{arr[2]}, arr[1], null, null);
                                }
                                else
                                    m = (MatrixCursor) query(mUri, null , arr[1], null, null);

                                break;
                            case 2:
                                cv = new ContentValues();
                                cv.put(KEY_FIELD, arr[1]);
                                cv.put(VALUE_FIELD, arr[2]);
                                insert(mUri, cv);
                                break;
                            case 3:
                                delete(mUri, arr[1], null);
                                break;
                        }
                        if (Integer.parseInt(arr[0]) == 1) {
                            try {
                                os = socket.getOutputStream();
                                out = new PrintWriter(os, true);
                                out.println(cursorToString(m));
                                out.flush();
                            }finally{
                                out.close();
                                os.close();
                            }
                        }

                    }

                } catch (Exception e) {

                } finally {
                    try {
                        br.close();
                        is.close();
                        socket.close();
                    } catch (Exception e) {

                    }

                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket = null;
            BufferedReader br = null;
            PrintWriter out = null;
            OutputStream os = null;
            OutputStreamWriter osw = null;
            BufferedWriter bw = null;
            Integer curPort = null;
            InputStream is = null;
            if(msgs[0]==null || msgs[0]=="")
                return null;
            String msgToSend ="";
            if(msgs[0].equals("0")){
                String socketHash = null;
                for (Iterator<Integer> portNo = remotePorts.iterator(); portNo.hasNext(); ) {
                    curPort = portNo.next();
                    if (curPort == Integer.parseInt(myPort)) {
                        continue;
                    }
                    try {

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), curPort);
                        Integer port = curPort/2;
                        os = socket.getOutputStream();
                        osw = new OutputStreamWriter(os);
                        out = new PrintWriter(osw, true);
                        out.println(msgs[0] + " " + portStr);
                        out.flush();
                        is = socket.getInputStream();
                        br = new BufferedReader(new InputStreamReader(is));
                        if (br.readLine().equalsIgnoreCase("yes")) {
                            try {
                                socketHash = genHash(String.valueOf(port));
                            } catch (Exception e) {
                                Log.e("Client Task", "Exception while generating hash for successor node ");
                            }


                            socketHM.put(port.toString(), socket);
                            nodeTree.put(socketHash, port.toString());
                        }
                    }catch(Exception e){

                    }finally {
                        try {
                                br.close();
                                is.close();
                                socket.close();
                            } catch (Exception e) {
                        }
                    }

                }
                updatePreSuc();
                return null;
            }
            for(int i=0;i<msgs.length;i++){
                msgToSend = msgToSend.concat(msgs[i]);
                msgToSend = msgToSend.concat(" ");
            }
            if(!successorNodeId.equals(myNodeId)) {
                Log.d("Client Task", "successorNodeId " + successorNodeId + " myNodeId " + myNodeId);
                try {
                    Log.d("Client Task", "Before socket creation successorPort"+successorPort);
                      successorNodeSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort));

                    socket = successorNodeSocket;
                    os = socket.getOutputStream();
                    osw = new OutputStreamWriter(os);
                    out = new PrintWriter(osw, true);
                    is = socket.getInputStream();
                    br = new BufferedReader(new InputStreamReader(is));
                    out.println(msgToSend);
                    Log.d("Client Task", "After sending message ");
                    out.flush();
                    Log.d("Client Task", "After flush ");
                    if (msgs[0].equals("1")) {
                        Log.d("Client Task", "Sent to successor port "+successorPort);
                        hmString = br.readLine();
                        Log.d("Client Task", "hmString after readLine"+hmString);
                    }
                } catch (Exception e) {
                    Log.e("Client Task", "Exception ");

                } finally {
                    try {

                        br.close();
                        is.close();
                        out.close();
                        osw.close();
                        os.close();
                        socket.close();
                    } catch (Exception e) {
                    }
                }
            }
            return null;
        }
    }
}
