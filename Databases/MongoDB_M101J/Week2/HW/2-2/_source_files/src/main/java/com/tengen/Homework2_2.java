package com.tengen;

import com.mongodb.*;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.Random;

import java.util.List;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: carloslazo
 * Date: 10/20/13
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */

public class Homework2_2 {
    public static void main(String[] args) throws UnknownHostException {

        // Initialize MongoDB Java Driver.

        MongoClient client = new MongoClient();
        DB db = client.getDB("students");
        DBCollection collection = db.getCollection("grades");

        // Initialize storage variables for computation.

        List<Object> score_delete = new ArrayList<Object>();
        ObjectId prev_score = new ObjectId();
        Integer  prev_id      = 0;

        // Initialize query and cursor.
        //  > Find all items which have type -> homework.
        //  > Sort by student_id and then by reverse score order.

        BasicDBObject query = new BasicDBObject("type","homework");
        DBCursor cursor = collection.find(query).sort(new BasicDBObject("student_id",1).append("score",-1));

        // Find and store all lowest homework values.

        try {
            while (cursor.hasNext()) {
                DBObject curr_score = cursor.next();
                Integer  curr_id      = (Integer)curr_score.get("student_id");

                if(!prev_id.equals(curr_id))
                    score_delete.add(prev_score);

                prev_id = curr_id;
                prev_score = (ObjectId)curr_score.get("_id");
            }

            score_delete.add(prev_score);
        } finally {
            cursor.close();
        }

        // Remove lowest scores based on their _id values.

        for (int i = 0; i < score_delete.size(); i++){
            collection.remove(new BasicDBObject("_id",score_delete.get(i)));
        }
    }
}