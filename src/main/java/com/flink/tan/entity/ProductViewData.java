package com.flink.tan.entity;

/**
 * @Title ProductViewData
 * @Author zhengqiang.tan
 * @Date 2020/10/27 8:45 PM
 */
public class ProductViewData {
    private String productId;
    private String user;
    private Long operationType;
    private Long timestamp;

    public ProductViewData(String productId, String user, Long operationType, Long timestamp) {
        this.productId = productId;
        this.user = user;
        this.operationType = operationType;
        this.timestamp = timestamp;
    }

    public String getProductId() {
        return productId;
    }

    public String getUser() {
        return user;
    }

    public Long getOperationType() {
        return operationType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setOperationType(Long operationType) {
        this.operationType = operationType;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ProductViewData{" +
                "product='" + productId + '\'' +
                ", user='" + user + '\'' +
                ", operationType=" + operationType +
                ", timestamp=" + timestamp +
                '}';
    }
}
