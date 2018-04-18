package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by shivamsahu on 3/18/18.
 */

public class SimpleDhtDbHelper extends SQLiteOpenHelper {

    private static final String DATABASE_NAME = "SimpleDhtDB.db";


    private static final int VERSION = 1;

    public SimpleDhtDbHelper(Context context) {
        super(context, DATABASE_NAME, null, VERSION);

    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        final String CREATE_TABLE = "CREATE TABLE "  + SimpleDhtContract.SimpleDhtEntry.TABLE_NAME + " (" +
                SimpleDhtContract.SimpleDhtEntry._ID  + " INTEGER PRIMARY KEY, " +
                SimpleDhtContract.SimpleDhtEntry.COLUMN_KEY + " TEXT NOT NULL UNIQUE, " +
                SimpleDhtContract.SimpleDhtEntry.COLUMN_VALUE    + " TEXT NOT NULL);";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + SimpleDhtContract.SimpleDhtEntry.TABLE_NAME);
        onCreate(db);
    }
}
