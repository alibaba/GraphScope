/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.prepare.store;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class PrepareStoreEntity implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareStoreEntity.class);
    private static final long serialVersionUID = -2362003390845698421L;

    private List<PrepareEntity> prepareEntityList;
    private TreeNodeLabelManager labelManager;
    private ValueType resultValueType;

    //queryFlow will not be serialized, which has been serialized by pb
    private transient QueryFlowOuterClass.QueryFlow queryFlow;

    public PrepareStoreEntity(List<PrepareEntity> prepareEntityList, TreeNodeLabelManager labelManager, ValueType resultValueType, QueryFlowOuterClass.QueryFlow queryFlow) {
        this.prepareEntityList = prepareEntityList;
        this.labelManager = labelManager;
        this.resultValueType = resultValueType;
        this.queryFlow = queryFlow;
    }

    public PrepareStoreEntity (byte[] bytes) {
        PrepareStoreEntity prepareStoreEntity = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            prepareStoreEntity = (PrepareStoreEntity) objectInputStream.readObject();

        } catch (Exception e) {
            LOGGER.error("Byte array to object PrepareStoreEntity failed, " + e);
            throw new RuntimeException(e);
        } finally {
            if (byteArrayInputStream != null ) {
                try {
                    byteArrayInputStream.close();
                } catch (IOException e){
                    LOGGER.error("Close byte array input stream failed, " + e);
                    throw new RuntimeException(e);
                }
            }
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    LOGGER.error("Close object input stream failed, " + e);
                    throw new RuntimeException(e);
                }
            }
        }

        this.prepareEntityList = prepareStoreEntity.prepareEntityList;
        this.labelManager = prepareStoreEntity.labelManager;
        this.resultValueType = prepareStoreEntity.resultValueType;
    }

    public List<PrepareEntity> getPrepareEntityList() {
        return prepareEntityList;
    }

    public TreeNodeLabelManager getLabelManager() {
        return labelManager;
    }

    public ValueType getResultValueType() {
        return resultValueType;
    }

    public QueryFlowOuterClass.QueryFlow getQueryFlow() { return queryFlow; }

    public void setQueryFlow(QueryFlowOuterClass.QueryFlow queryFlow) { this.queryFlow = queryFlow; }

    // transform the object PrepareStoryEntity to byte array, except queryFlow
    public byte[] toByteArray() {
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;

        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.flush();
            bytes = byteArrayOutputStream.toByteArray();

        } catch (IOException e) {
            LOGGER.error("Transform object PrepareStoreEntity to byte array failed, " + e);
            throw new RuntimeException(e);
        } finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    LOGGER.error("Close byte array output stream failed, " + e);
                }
            }
        }

        return bytes;
    }

    // Transform the byte array to PrepareStoreEntity, which does't include queryFlow
    public static PrepareStoreEntity toPrepareStoreEntity(byte[] bytes) {
        PrepareStoreEntity prepareStoreEntity = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            prepareStoreEntity = (PrepareStoreEntity) objectInputStream.readObject();

        } catch (Exception e) {
            LOGGER.error("Byte array to object PrepareStoreEntity failed, " + e);
            throw new RuntimeException(e);
        } finally {
            if (byteArrayInputStream != null ) {
                try {
                    byteArrayInputStream.close();
                } catch (IOException e){
                    LOGGER.error("Close byte array input stream failed, " + e);
                }
            }
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    LOGGER.error("Close object input stream failed, " + e);
                }
            }
        }

        return prepareStoreEntity;
    }

}
