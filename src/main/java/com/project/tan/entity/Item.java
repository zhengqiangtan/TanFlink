package com.project.tan.entity;

/**
 * 商品实体
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/19 4:58 PM
 * @Version 1.0
 */
public class Item {
    private String name;
    private Integer id;

    public Item() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override

    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';

    }
}
