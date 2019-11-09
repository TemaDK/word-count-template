package org.ivan.experiments.entities;

public class Attribute {
    //порядок атрибута в файле
    private long order;
    //имя поля
    private String fieldName;
    //полное имя атрибута
    private String description;

    //конструктор класса
    public Attribute(long order, String fieldName, String description) {
        this.order = order;
        this.fieldName = fieldName;
        this.description = description;
    }


    /*
    методы get и set для получения и изменения полей класса
     */

    public long getOrder() {
        return order;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getDescription() {
        return description;
    }
}
