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
package com.alibaba.maxgraph.sdkcommon.util;

import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.exception.QueueRejectException;

public class ExceptionUtils {

    public enum ErrorCode {

        OK(0),
        PropertyAlreadyExist(1000),
        PropertyNotExist(1001),
        TypeAlreadyExist(1002),
        TypeNotFound(1003),
        RelationShipExistWithType(1004),
        RelationShipAlreadyExist(1005),
        PropertyNameAlreadyExist(1006),
        UnSupportedMetaOperation(1007),
        PropertyExistInType(1008),
        SchemaPersistentError(1009),
        DataTypeNotValid(1010),
        DefaultValueNotMatchDataType(1011),
        DefaultValueNotSupportedForDataType(1012),
        RelationShipNotExist(1013),
        InvalidTypeChanged(1014),

        IndexCanBeUsedOnlyOnEdge(1100),
        IndexTypeMustUnique(1101),

        // client-frontend related
        IllegalSession(2000),
        FrontendServiceBusy(2001),
        SessionTimeout(2002),
        RealtimeWriteFailed(2003),
        ServiceNotReady(2004),
        RealtimeDataNotValid(2005),
        QueueReject(2006),

        JobNotExist(3000),
        JobIsTerminated(3001),
        JobIsAlreadyInSnapshot(3002),
        JobIdIsAlreadyExistInGraph(3003),
        JobIdMustGreaterThanBefore(3004),
        JobQueueIsFullForThisType(3005),

        SnapshotPersistentError(4000),

        // bulk load related
        IllegalVertexLabel(5000),
        IllegalEdgeLabel(5001),
        IllegalSrcDstLabel(5002),
        IllegalPlatform(5003),
        IllegalStoreType(5004),
        IllegalPrimaryKey(5005),
        IllegalProperty(5006),
        IllegalParameter(5007),
        BuildDataFailed(5008),
        OnlineDataFailed(5009),
        SubmitBuildJobFailed(5010),
        CancelBuildFailed(5011),
        CancelOnlineFailed(5012),
        CancelJobFailed(5013),
        HttpRequestFailed(5014),

        //frontend related
        AuthenticationFailed(6000),
        AuthorizationFailed(6001),

        // studio related
        @Deprecated
        AkAuthenticatedFailed(7000),

        UnsupportedFilter(7001),
        InstanceNotFound(7002),
        DeleteInstanceFailed(7003),
        InvalidParams(7004),
        CannotRename(7005),
        ClusterNotFound(7006),
        AddPackageVersionFailed(7007),
        InstanceNameExist(7008),
        OneAdminNeededByInstance(7009),
        MonitorClusterFailed(7010),
        MonitorInstanceFailed(7011),
        ApiNoPermission(7012),
        CreateVpcAlbFailed(7013),
        DeleteVpcAlbFailed(7014),
        NoAvailablePackage(7015),
        AkAccessKeyNotFound(7016),
        AkSignatureFailed(7017),
        AkSignatureMethodUnSupported(7018),
        AkTimestampIllegal(7019),
        AkSignatureNotFound(7020),
        AkParameterInvalid(7021),
        AkClientNotConnected(7022),
        InvalidInstanceName(7023),
        ResourceInsufficient(7024),

        Unknown(9999);

        private int value;

        ErrorCode(int value) {
            this.value = value;
        }

        public int toInt() {
            return value;
        }

        public static ErrorCode fromInt(int code) {
            for (ErrorCode errorCode : ErrorCode.class.getEnumConstants()) {
                if (errorCode.toInt() == code) {
                    return errorCode;
                }
            }

            return ErrorCode.Unknown;
        }
    }

    public static void checkAndThrow(int errCode, String errMsg) throws MaxGraphException {
        ErrorCode errorCode = ErrorCode.fromInt(errCode);
        switch (errorCode) {
            case OK:
                return;
            case Unknown:
                throw new RuntimeException(errMsg);
            case QueueReject:
                throw new QueueRejectException(errMsg);
            default:
                throw new MaxGraphException(errorCode, errMsg);

        }
    }
}
