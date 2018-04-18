package edu.buffalo.cse.cse486586.simpledht;

import java.util.Comparator;

/**
 * Created by shivamsahu on 3/18/18.
 */

class Nodes {

    public String port;
    public String ID;

    public String getPort() {
        return port;
    }

    public void setPort(int String) {
        this.port = port;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public Nodes(String port, String ID) {
        this.port = port;
        this.ID = ID;

    }

    public static class NodesComparator implements Comparator<Nodes> {
        @Override
        public int compare(Nodes lhs, Nodes rhs) {
            if(lhs==null){
                return 1;
            } else if(rhs==null){
                return -1;
            }
            return lhs.ID.compareTo(rhs.ID);
        }
    }
}
