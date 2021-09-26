package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A simple extending with a more user friendly name, and let incoming/outgoing message type be
 * the same
 */
public abstract class BasicComputation<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, MSG_T extends Writable>
    extends AbstractComputation<OID_T,VDATA_T,EDATA_T,MSG_T,MSG_T>{

}
