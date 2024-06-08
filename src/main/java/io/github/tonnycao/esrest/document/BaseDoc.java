package io.github.tonnycao.esrest.document;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;


@Data
public class BaseDoc implements Serializable {

    private String id;

    private String tag;

    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date createdAt;

    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date updatedAt;
}
