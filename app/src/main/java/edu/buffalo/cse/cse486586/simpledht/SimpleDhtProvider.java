package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;

public class SimpleDhtProvider extends ContentProvider {

    private SimpleDhtDbHelper mSimpleDhtDbHelper;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String myPort;
    ArrayList<Nodes> nodeArrayList = null;
    static final int SERVER_PORT = 10000;
    Nodes MyNode,successorNode,predecessorNode;
    boolean flag_5554 = false;
    boolean alone = true;
    MatrixCursor cursorSingleRecord;
    boolean querySingleRecord = false;
    MatrixCursor cursorALLRecord;
    boolean queryALLRecord = false;


    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final SQLiteDatabase db = mSimpleDhtDbHelper.getWritableDatabase();
        Uri result;
        String key = values.getAsString("key").trim();
        String value = values.getAsString("value").trim();
        if(InRange(key)){
            long id = db.insertWithOnConflict(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME,null ,values, CONFLICT_REPLACE);
            if ( id > 0 ) {
                result = ContentUris.withAppendedId(uri, id);
            } else {
                throw new android.database.SQLException("Failed to insert row into " + uri);
            }
            Log.v("insert#",values.toString());
            return result;
        }else{
            String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNode.getPort().trim()+"##"+key+"##"+value;
            //String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorNode.getPort().trim());
            return uri;
        }

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        final SQLiteDatabase db = mSimpleDhtDbHelper.getReadableDatabase();
        Cursor result;
        Log.v(TAG,"Selection Key == "+selection);
        if(selection.equals("@")){   // AVD local

            result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, projection, null,
                    null, null, null, sortOrder);
            result.moveToNext();
            Log.v("query", selection);
            return result;

        }else if(selection.equals("*") && alone){ // AVD local(bcz only one avd)

            result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, projection, null,
                    null, null, null, sortOrder);
            result.moveToNext();
            Log.v("query", selection);
            return result;

        }else if(selection.equals("*") && !alone){ //entire DHT

            cursorALLRecord = new MatrixCursor(new String[]{"key","value"});
            result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, projection, null,
                    null, null, null, sortOrder);
            while(result.moveToNext()){
                String key1 = result.getString(1);
                String value1 = result.getString(2);
                cursorALLRecord.addRow(new String[]{key1, value1});
            }
            String msgToSend = "AllQueryFind"+"##"+myPort.trim()+"##"+successorNode.getPort().trim();
            //String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorNode.getPort().trim());
            queryALLRecord = false;
            while(!queryALLRecord)
            {
                //wait for result;
            }
            Log.v(TAG,"Waiting end: sending response");
            return cursorALLRecord;
        }
        if(alone){

            String mSelection = SimpleDhtContract.SimpleDhtEntry.COLUMN_KEY + "=?";
            String[] mSelectionArgs = new String[]{selection};
            result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, projection, mSelection,
                    mSelectionArgs, null, null, sortOrder);
            result.moveToNext();
            Log.v("query", selection);
            return result;
        }

        if(InRange(selection)){
            Log.v(TAG,"Inside query: In My Range");
            String mSelection = SimpleDhtContract.SimpleDhtEntry.COLUMN_KEY + "=?";
            String[] mSelectionArgs = new String[]{selection};
            result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, projection, mSelection,
                    mSelectionArgs, null, null, sortOrder);
            result.moveToNext();
            Log.v("query", selection);
            return result;

        } else {

            Log.v(TAG,"Inside query: outside My Range");
            cursorSingleRecord = new MatrixCursor(new String[]{"key","value"},1);
            String msgToSend = "SingleQueryFind"+"##"+myPort.trim()+"##"+successorNode.getPort().trim()+"##"+selection;
            //String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorNode.getPort().trim());
            querySingleRecord = false;
            while(!querySingleRecord)
            {
                //wait for result;
            }
            Log.v(TAG,"Waiting end: sending response");
            return cursorSingleRecord;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mSimpleDhtDbHelper.getReadableDatabase();
        int result;
        if(selection.equals("@")) {   // AVD local
            result = db.delete(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, null, null);
            return result;
        }else if(selection.equals("*") && alone) { // AVD local(bcz only one avd)
            result = db.delete(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, null, null);
            return result;
        }else if(selection.equals("*") && !alone) { // entire DHT
            result = 0;
            return result;
        }else{
            String mSelection = SimpleDhtContract.SimpleDhtEntry.COLUMN_KEY + "=?";
            String[] mSelectionArgs = new String[]{selection};
            result = db.delete(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, mSelection, mSelectionArgs);
            return result;
        }
    }

    @Override
    public boolean onCreate() {
        mSimpleDhtDbHelper = new SimpleDhtDbHelper(getContext());
        Log.v(TAG, "Started server");
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            if (Integer.parseInt(myPort) == 11108) {
                MyNode = new Nodes(myPort, genHash(String.valueOf(Integer.parseInt(myPort)/2)));
                predecessorNode = new Nodes(MyNode.port, MyNode.ID);
                successorNode = new Nodes(MyNode.port, MyNode.ID);
                flag_5554 = true;
                nodeArrayList = new ArrayList<Nodes>(10);
                nodeArrayList.add(MyNode);
                Log.v(TAG, "Inside onCreate 5554 MyNode: " + MyNode.toString());
            } else {
                MyNode = new Nodes(myPort, genHash((String.valueOf(Integer.parseInt(myPort)/2))));
                predecessorNode = new Nodes(MyNode.port, MyNode.ID);
                successorNode = new Nodes(MyNode.port, MyNode.ID);
                Log.v(TAG, "I onCreate MyNode: " + MyNode.toString());
                String msgToSend = "Introduce"+"##"+myPort.trim()+"##"+"11108";
                //String msgToSend = "Introduce ## Source ## Destination";
                String destinationPort = "11108";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, destinationPort);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.v(TAG, "" + e);
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        return true;
    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        return 0;
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

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private boolean InRange(String key) {
        if(alone){
            return true;
        }
        try {
            String hashedKey = genHash(key);
            if(predecessorNode.ID.compareTo(MyNode.ID) > 0 &&  hashedKey.compareTo(MyNode.ID) <= 0 && hashedKey.compareTo(predecessorNode.ID) < 0 ||
                    predecessorNode.ID.compareTo(MyNode.ID) > 0 && hashedKey.compareTo(predecessorNode.ID) > 0 && hashedKey.compareTo(MyNode.ID) > 0 ||
                    hashedKey.compareTo(predecessorNode.ID) > 0 && hashedKey.compareTo(MyNode.ID) <= 0){
                Log.v(TAG,"key is in my range:: " + key );
                return true;
            } else {
                Log.v(TAG,"key is not in my range: " + key);
                return false;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.v(TAG, "" + e);
        }
        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            //Log.v(TAG, "Inside ServerTask doInBackground");
            ServerSocket serverSocket = sockets[0];
            //Log.v(TAG, "Inside ServerTask doInBackground serverSocket: "+serverSocket);

            try {
                while (true) {
                    Log.v(TAG, "Inside while true");
                /* Following code is reference from https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html*/
                    Socket inputSocket = serverSocket.accept();
                    InputStream inputStream = inputSocket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String recvMsg = bufferedReader.readLine();
                    Log.v(TAG, "Inside ServerTask Recv msg: "+recvMsg);
                    String[] msgSplit = recvMsg.split("##");
                    if(msgSplit[0].equalsIgnoreCase("Introduce") && flag_5554){
                        Log.v(TAG, "In Server Introduce");
                        Nodes newNode = new Nodes(msgSplit[1], genHash(String.valueOf(Integer.parseInt(msgSplit[1])/2)));
                        nodeArrayList.add(newNode);
                        Collections.sort(nodeArrayList, new Nodes.NodesComparator());
                        Log.v(TAG, nodeArrayList.toString());
                        //String msgToSend = "Introduce ## Source ## Destination ## ";

                        Nodes temp[] = new Nodes[nodeArrayList.size()];
                        nodeArrayList.toArray(temp);
                        for (int i = 0; i < temp.length; i++) {
                            String destinationPort = temp[i].getPort().trim();
                            String successorNodePort;
                            String predecessorNodePort;
                            if (i == 0) {
                                predecessorNodePort = temp[temp.length-1].getPort().trim();
                            } else {
                                predecessorNodePort = temp[i-1].getPort().trim();
                            }
                            if (i == temp.length - 1) {
                                successorNodePort = temp[0].getPort().trim();
                            } else {
                                successorNodePort = temp[i+1].getPort().trim();
                            }
                            String msgToSend = "Update"+"##"+myPort.trim()+"##"+destinationPort+"##"+predecessorNodePort+"##"+successorNodePort;
                            //String msgToSend = "Introduce ## Source ## Destination ## Predecessor ## Successor";
                            Log.v(TAG, "In Server Introduce msgToSend: " +msgToSend);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, destinationPort);
                        }

                    }else if(msgSplit[0].equalsIgnoreCase("Update")){
                        alone = false;
                        Log.v(TAG,"Alone is false");
                        Log.v(TAG, "In Server Update");
                        predecessorNode = new Nodes(msgSplit[3], genHash(String.valueOf(Integer.parseInt(msgSplit[3])/2)));
                        successorNode = new Nodes(msgSplit[4], genHash(String.valueOf(Integer.parseInt(msgSplit[4])/2)));
                        Log.v(TAG, "In Server Update predecessorNode"+ predecessorNode.getPort());
                        Log.v(TAG, "In Server Update Successor"+ successorNode.getPort());
                        // publishProgress(new String[]{predecessorNode.getPort(), successorNode.getPort()});
                    }else if(msgSplit[0].equalsIgnoreCase("Insert")){
                        if(InRange(msgSplit[3])){
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentValues mContentValues = new ContentValues();
                            mContentValues.put("key", msgSplit[3]);
                            mContentValues.put("value", msgSplit[4]);
                            getContext().getContentResolver().insert(mUri, mContentValues);
                        } else {
                            String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNode.getPort().trim()+"##"+msgSplit[3]+"##"+msgSplit[4];
                            //String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorNode.getPort());
                        }
                    }else if(msgSplit[0].equalsIgnoreCase("SingleQueryFind")){
                        Log.v(TAG,"Inside Server: SingleQueryFind");
                        String key = msgSplit[3];
                        //String hashedkey = genHash(key);
                        if(InRange((key))){
                            Log.v(TAG,"Inside Server: SingleQueryFind : In my Range");
                            SQLiteDatabase db = mSimpleDhtDbHelper.getReadableDatabase();
                            String mSelection = SimpleDhtContract.SimpleDhtEntry.COLUMN_KEY + "=?";
                            String[] mSelectionArgs = new String[]{key};

                            Cursor result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, null, mSelection,
                                    mSelectionArgs, null, null, null);
                            result.moveToNext();
                            String key1 = result.getString(1);
                            String value1 = result.getString(2);
                            String msgToSend = "SingleQueryResponse"+"##"+myPort.trim()+"##"+msgSplit[1].trim()+"##"+key1.trim()+"##"+value1.trim();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, msgSplit[1].trim());
                        }else{
                            Log.v(TAG,"Inside Server: SingleQueryFind : ouside my Range");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recvMsg, successorNode.getPort().trim());
                        }
                    }else if(msgSplit[0].equalsIgnoreCase("SingleQueryResponse")){
                        Log.v(TAG,"Inside SingleQueryResponse: "+recvMsg);
                        cursorSingleRecord.addRow(new String[]{msgSplit[3], msgSplit[4]});
                        querySingleRecord = true;
                    }else if(msgSplit[0].equalsIgnoreCase("AllQueryFind")) {
                        Log.v(TAG,"Inside AllQueryFind: "+recvMsg);
                        if(msgSplit[1].trim().equalsIgnoreCase(msgSplit[2].trim())){
                            queryALLRecord = true;
                        }else{
                            SQLiteDatabase db = mSimpleDhtDbHelper.getReadableDatabase();
                            Cursor result = db.query(SimpleDhtContract.SimpleDhtEntry.TABLE_NAME, null, null,
                                    null, null, null, null);
                            String output = "AllQueryResponse";
                            while(result.moveToNext()){
                                String key1 = result.getString(1);
                                String value1 = result.getString(2);
                                output=  output +"##"+key1.trim()+"##"+value1.trim();
                            }
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, output, msgSplit[1].trim());
                            String forwardMsg = "AllQueryFind"+"##"+msgSplit[1].trim()+"##"+successorNode.getPort().trim();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forwardMsg,successorNode.getPort().trim());
                        }
                    }else if(msgSplit[0].equalsIgnoreCase("AllQueryResponse")) {
                        Log.v(TAG,"Inside AllQueryResponse:");
                        for(int i = 1; i < msgSplit.length; i += 2){
                            cursorALLRecord.addRow(new String[]{msgSplit[i], msgSplit[i+1]});
                        }
                    }

                }
            }catch (IOException e){
                Log.v(TAG, "In ServerTask  IOException");
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                Log.v(TAG, "In ServerTask  NoSuchAlgorithmException");
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... receivedMsg) {

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            Log.v(TAG, "Inside Client Task");
            String msgToSend = params[0];
            String destinationPort = params[1];
            try {
                Socket socket;
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort));
                Log.v(TAG, "In ClientTask Sending = " +msgToSend);
                OutputStream outputStream = socket.getOutputStream();
                PrintWriter printWriter = new PrintWriter(outputStream, true);
                printWriter.println(msgToSend);
                socket.close();
            } catch (UnknownHostException e) {
                Log.v(TAG, "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.v(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            return null;
        }
    }
}
