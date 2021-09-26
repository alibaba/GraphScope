package com.alibaba.graphscope.parallel.mm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;

public class ListMessageIterable<MSG_T extends Writable> implements MessageIterable<MSG_T>{
    public static int DEFAULT_MESSAGE_ITERABLE_SIZE = 4;

    private List<MSG_T> msgs;

    public ListMessageIterable(){
        msgs = new ArrayList<MSG_T>(DEFAULT_MESSAGE_ITERABLE_SIZE);
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<MSG_T> iterator() {
        return msgs.iterator();
    }


    public void append(MSG_T msg){
        msgs.add(msg);
    }

    public void clear(){
        msgs.clear();
    }

    public int size(){
        return msgs.size();
    }
}
