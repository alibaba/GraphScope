package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.FixedChar;
import com.alibaba.graphscope.groot.service.models.FixedCharChar;
import com.alibaba.graphscope.groot.service.models.LongText;
import com.alibaba.graphscope.groot.service.models.VarChar;
import com.alibaba.graphscope.groot.service.models.VarCharVarChar;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;


@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-01-07T12:01:45.705446+08:00[Asia/Shanghai]")
public interface StringTypeString {
}
