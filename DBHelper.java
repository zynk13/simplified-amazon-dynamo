package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by aravind on 5/9/17.
 */

public class DBHelper extends SQLiteOpenHelper {

    private static final String Sql_create_Db = "CREATE TABLE db(key TEXT PRIMARY KEY, value TEXT, version INTEGER, partition INTEGER)";

    DBHelper(Context context, String DBNAME) {

        super(context, DBNAME, null, 1);
    }

    public void onCreate(SQLiteDatabase db){

        db.execSQL(Sql_create_Db);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int i, int i1) {

    }
}
