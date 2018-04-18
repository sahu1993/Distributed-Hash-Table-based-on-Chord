package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

/**
 * Created by shivamsahu on 3/18/18.
 */

public class SimpleDhtContract {

    public static class SimpleDhtEntry implements BaseColumns {

        // Table and Table columns
        public static final String TABLE_NAME = "group_messenger";
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";

    }
}
